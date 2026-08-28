#!/usr/bin/env python3
"""Workshop setup script for the Databricks State & Local workshop.

Creates an autoscaling Lakebase project, loads ``medical_providers`` into
``databricks_postgres.public``, registers that database as a Unity Catalog
Lakebase catalog, creates shared UC resources (``main`` catalog / TMSIS volume),
uploads ``tmsis_claims.json``, creates per-participant catalogs
(``{username}_dev`` with bronze/silver/gold), and provisions workspace users.

When the workspace has UC storage credentials defined, the script also
provisions an S3 bucket (us-west-2), IAM role, and external location for the
shared catalog (requires AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
AWS_SESSION_TOKEN). When no storage credentials exist, catalogs are created
without a managed location and no AWS resources are provisioned.

The Databricks CLI profile from ~/.databrickscfg can be passed with
``--profile/-p`` (e.g. ``--profile sl-workshop``); if omitted, you are prompted
to choose one.

Requires: databricks-sdk, boto3, psycopg, pyarrow  (pip install -r requirements.txt)
"""

from __future__ import annotations

import argparse
import base64
import configparser
import csv
import io
import json
import os
import re
import sys
import time
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import TypedDict

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[misc, assignment]

try:
    import psycopg
except ImportError:
    psycopg = None  # type: ignore[assignment]

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None  # type: ignore[assignment]

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.common.lro import LroOptions
    from databricks.sdk.errors import AlreadyExists, NotFound, ResourceAlreadyExists
    from databricks.sdk.service import iam, postgres
    from databricks.sdk.service.catalog import (
        AwsIamRoleRequest,
        PermissionsChange,
        Privilege,
        VolumeType,
    )
    from databricks.sdk.service.sql import (
        ExecuteStatementRequestOnWaitTimeout,
        StatementState,
    )
    from databricks.sdk.service.workspace import ImportFormat
except ImportError:
    print("Error: databricks-sdk is required.  pip install databricks-sdk")
    sys.exit(1)


# Shared catalog used by Lab 1.3: /Volumes/main/default/tmsis_claims/
DEFAULT_SHARED_CATALOG = "main"
DEFAULT_SHARED_SCHEMA = "default"
WORKSHOP_REPO_URL = "https://github.com/scottblackdb/databricks-sl-workshop"
WORKSHOP_REPO_WORKSPACE_PATH = "/Shared/workshop_repo.txt"
DEFAULT_TMSIS_VOLUME = "tmsis_claims"
DEFAULT_TMSIS_FILE = "tmsis_claims.json"
DEFAULT_TMSIS_LOCAL_PATH = Path(__file__).resolve().parent / DEFAULT_TMSIS_FILE

# Lakebase / Lab 1.1 source: medical_providers.public.medical_providers
DEFAULT_LAKEBASE_NAME = "SLED Workshop"
DEFAULT_LAKEBASE_DATABASE = "databricks_postgres"
DEFAULT_LAKEBASE_SCHEMA = "public"
DEFAULT_LAKEBASE_TABLE = "medical_providers"
DEFAULT_LAKEBASE_UC_CATALOG = "medical_providers"
DEFAULT_MEDICAL_PROVIDERS_PARQUET = (
    Path(__file__).resolve().parent / "medical_providers.parquet"
)
DEFAULT_MEDICAL_PROVIDERS_SQL = (
    Path(__file__).resolve().parent / "medical_providers.sql"
)

DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"

# Unity Catalog on AWS: Databricks master role that assumes customer IAM roles.
UC_AWS_MASTER_ROLE_ARN = (
    "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
)
AWS_UC_REGION = "us-west-2"
UC_PLACEHOLDER_EXTERNAL_ID = "0000"

ALL_USERS_PRINCIPAL = "account users"
WORKSHOP_ENTITLEMENTS = ("workspace-access", "databricks-sql-access")
IAM_ROLE_PROPAGATION_SECONDS = 10

_SQL_TERMINAL_STATES = frozenset({
    StatementState.SUCCEEDED,
    StatementState.FAILED,
    StatementState.CANCELED,
    StatementState.CLOSED,
})

_MEDICAL_PROVIDERS_COLUMNS = (
    "medicaid_provider_id",
    "npi",
    "provider_or_facility_name",
    "medicaid_type",
    "profession_or_service",
    "provider_specialty",
    "service_address",
    "city",
    "state",
    "zip_code",
    "county",
    "telephone",
    "latitude",
    "longitude",
    "enrollment_begin_date",
    "next_anticipated_revalidation_date",
    "file_date",
    "medically_fragile_children_and_adults_directory_ind",
    "provider_email",
)

_MEDICAL_PROVIDERS_DDL = """
CREATE TABLE IF NOT EXISTS public.medical_providers (
    medicaid_provider_id INTEGER,
    npi BIGINT,
    provider_or_facility_name TEXT,
    medicaid_type TEXT,
    profession_or_service TEXT,
    provider_specialty TEXT,
    service_address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county TEXT,
    telephone BIGINT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    enrollment_begin_date DATE,
    next_anticipated_revalidation_date DATE,
    file_date DATE,
    medically_fragile_children_and_adults_directory_ind TEXT,
    provider_email TEXT
)
""".strip()


class AwsUcStorage(TypedDict):
    external_location_url: str
    managed_location: str
    storage_credential_name: str
    external_location_name: str


def _is_already_exists(exc: Exception) -> bool:
    if isinstance(exc, (AlreadyExists, ResourceAlreadyExists)):
        return True
    msg = str(exc).lower()
    return any(k in msg for k in ("already exists", "conflict", "409", "uniqueness"))


def _create_idempotent(
    create_fn: Callable[[], None],
    *,
    created_msg: str,
    exists_msg: str,
    error_label: str,
) -> None:
    try:
        create_fn()
        print(f"  [+] {created_msg}")
    except (AlreadyExists, ResourceAlreadyExists):
        print(f"  [~] {exists_msg}")
    except Exception as e:
        if _is_already_exists(e):
            print(f"  [~] {exists_msg}")
        else:
            print(f"  [!] {error_label} — {e}")
            sys.exit(1)


def _grant_privileges(
    w: WorkspaceClient,
    *,
    securable_type: str,
    full_name: str,
    privileges: list[Privilege],
    principal: str = ALL_USERS_PRINCIPAL,
    description: str,
) -> None:
    try:
        w.grants.update(
            securable_type=securable_type,
            full_name=full_name,
            changes=[
                PermissionsChange(
                    principal=principal,
                    add=privileges,
                )
            ],
        )
        privs = ", ".join(p.value for p in privileges)
        print(
            f"  [+] grants on {description} '{full_name}' to '{principal}': {privs}"
        )
    except Exception as e:
        print(f"  [!] grant on {description} '{full_name}' — {e}")
        sys.exit(1)


def _normalize_storage_url(url: str) -> str:
    return url.rstrip("/") + "/"


def _client_error_code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


def get_profiles() -> list[str]:
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        print("Error: ~/.databrickscfg not found. Run 'databricks configure' first.")
        sys.exit(1)
    config = configparser.RawConfigParser()
    config.read(cfg_path)
    return config.sections()


def select_profile(profiles: list[str], profile_arg: str | None) -> str:
    if profile_arg:
        if profile_arg not in profiles:
            print(f"Error: profile '{profile_arg}' not found in ~/.databrickscfg")
            print(f"Available: {', '.join(profiles)}")
            sys.exit(1)
        return profile_arg

    if len(profiles) == 1:
        print(f"Using profile: {profiles[0]}")
        return profiles[0]

    print("\nAvailable Databricks profiles:")
    for i, profile in enumerate(profiles, 1):
        print(f"  {i}. {profile}")

    prompt = f"Please enter a number between 1 and {len(profiles)}"
    while True:
        try:
            choice = input(f"\nSelect profile [1-{len(profiles)}]: ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(profiles):
                return profiles[idx]
        except ValueError:
            pass
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(1)
        print(prompt)


# ---------------------------------------------------------------------------
# Lakebase (project, seed medical_providers, UC catalog)
# ---------------------------------------------------------------------------

def _to_project_id(name: str) -> str:
    """Sanitize a display name into a valid Lakebase project ID."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    if not slug or not slug[0].isalpha():
        slug = "ws-" + slug
    return slug[:63]


def _lakebase_project_resource_name(project_id: str) -> str:
    if project_id.startswith("projects/"):
        return project_id
    return f"projects/{project_id}"


def _lakebase_project_exists(w: WorkspaceClient, project_id: str) -> bool:
    try:
        w.postgres.get_project(name=_lakebase_project_resource_name(project_id))
        return True
    except NotFound:
        return False
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in ("not found", "does not exist", "404")):
            return False
        raise


def _grant_lakebase_project_manage_all_account_users(
    w: WorkspaceClient, project_id: str
) -> None:
    try:
        w.permissions.update(
            request_object_type="database-projects",
            request_object_id=project_id,
            access_control_list=[
                iam.AccessControlRequest(
                    group_name=ALL_USERS_PRINCIPAL,
                    permission_level=iam.PermissionLevel.CAN_MANAGE,
                )
            ],
        )
        print(
            f"  [+] Lakebase project '{project_id}': CAN_MANAGE for "
            f"group '{ALL_USERS_PRINCIPAL}' (All account users)"
        )
    except Exception as e:
        print(f"  [!] Lakebase project permissions on '{project_id}' — {e}")
        sys.exit(1)


def create_lakebase(
    w: WorkspaceClient,
    display_name: str,
    pg_version: int = 17,
    *,
    skip_if_exists: bool = False,
) -> str:
    """Create an autoscaling Lakebase project. Returns the project id."""
    project_id = _to_project_id(display_name)
    if skip_if_exists and _lakebase_project_exists(w, project_id):
        print(
            f"\nSkipping Lakebase project '{display_name}' "
            f"(id: {project_id}) — already exists."
        )
        return project_id

    if _lakebase_project_exists(w, project_id):
        print(f"\nLakebase project '{display_name}' (id: {project_id}) already exists.")
        if not skip_if_exists:
            _grant_lakebase_project_manage_all_account_users(w, project_id)
        return project_id

    print(
        f"\nCreating autoscaling Lakebase project '{display_name}' "
        f"(id: {project_id}, pg{pg_version})..."
    )
    project = postgres.Project(
        spec=postgres.ProjectSpec(
            display_name=display_name,
            pg_version=pg_version,
            default_endpoint_settings=postgres.ProjectDefaultEndpointSettings(
                suspend_timeout_duration=postgres.Duration(seconds=300),
            ),
        )
    )
    try:
        op = w.postgres.create_project(project=project, project_id=project_id)
    except (AlreadyExists, ResourceAlreadyExists):
        print(f"  [~] Lakebase project '{project_id}' already exists")
        if not skip_if_exists:
            _grant_lakebase_project_manage_all_account_users(w, project_id)
        return project_id
    except Exception as e:
        if _is_already_exists(e):
            print(f"  [~] Lakebase project '{project_id}' already exists")
            if not skip_if_exists:
                _grant_lakebase_project_manage_all_account_users(w, project_id)
            return project_id
        print(f"  Error: {e}")
        sys.exit(1)

    print("  Waiting for Lakebase to become ready", end="", flush=True)
    try:
        result = op.wait(LroOptions(timeout=timedelta(seconds=600)))
        print()
        name = result.spec.display_name if result and result.spec else project_id
        print(f"  Ready: {name}")
        _grant_lakebase_project_manage_all_account_users(w, project_id)
    except Exception as e:
        print(f"\n  Operation ended: {e}")
        sys.exit(1)
    return project_id


def _resolve_lakebase_branch(w: WorkspaceClient, project_id: str) -> str:
    project = w.postgres.get_project(name=_lakebase_project_resource_name(project_id))
    branch = project.status.default_branch if project.status else None
    if branch:
        return branch
    branches = list(
        w.postgres.list_branches(parent=_lakebase_project_resource_name(project_id))
    )
    if not branches:
        print(f"Error: no branches found for Lakebase project '{project_id}'")
        sys.exit(1)
    return branches[0].name or ""


def _resolve_lakebase_endpoint(w: WorkspaceClient, branch: str) -> tuple[str, str]:
    """Return (endpoint_resource_name, postgres_host) for the RW endpoint."""
    endpoints = list(w.postgres.list_endpoints(parent=branch))
    chosen = None
    for endpoint in endpoints:
        etype = endpoint.status.endpoint_type if endpoint.status else None
        if etype == postgres.EndpointType.ENDPOINT_TYPE_READ_WRITE:
            chosen = endpoint
            break
    if chosen is None and endpoints:
        chosen = endpoints[0]
    if chosen is None or not chosen.name:
        print(f"Error: no Lakebase endpoints found on branch '{branch}'")
        sys.exit(1)
    host = None
    if chosen.status and chosen.status.hosts:
        host = chosen.status.hosts.host
    if not host:
        print(f"Error: Lakebase endpoint '{chosen.name}' has no host")
        sys.exit(1)
    return chosen.name, host


def _require_psycopg() -> None:
    if psycopg is None:
        print("Error: psycopg is required for Lakebase seeding.  pip install 'psycopg[binary]'")
        sys.exit(1)


def _require_pyarrow() -> None:
    if pq is None:
        print("Error: pyarrow is required to load medical_providers.parquet.  pip install pyarrow")
        sys.exit(1)


def _connect_lakebase(
    w: WorkspaceClient,
    *,
    endpoint_name: str,
    host: str,
    database: str,
):
    _require_psycopg()
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    user = w.current_user.me().user_name
    if not user:
        print("Error: could not resolve current user for Lakebase connection")
        sys.exit(1)
    return psycopg.connect(
        dbname=database,
        user=user,
        password=cred.token,
        host=host,
        port=5432,
        sslmode="require",
    )


def _load_medical_providers_table(
    conn,
    parquet_path: Path,
    *,
    force_reload: bool = False,
) -> None:
    _require_pyarrow()
    if not parquet_path.is_file():
        print(f"  [!] medical providers parquet not found: {parquet_path}")
        sys.exit(1)

    ddl = _MEDICAL_PROVIDERS_DDL
    if DEFAULT_MEDICAL_PROVIDERS_SQL.is_file():
        ddl = DEFAULT_MEDICAL_PROVIDERS_SQL.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'medical_providers'
            """
        )
        if cur.fetchone()[0] == 0:
            print("  [!] table public.medical_providers was not created")
            sys.exit(1)
        cur.execute("SELECT COUNT(*) FROM public.medical_providers")
        existing = cur.fetchone()[0]
        if existing and not force_reload:
            print(
                f"  [~] public.medical_providers already has {existing:,} rows — skipping load"
            )
            return
        if existing and force_reload:
            print(f"  [*] truncating public.medical_providers ({existing:,} rows)")
            cur.execute("TRUNCATE TABLE public.medical_providers")

    print(f"  Loading {parquet_path.name} into public.medical_providers ...")
    table = pq.read_table(parquet_path)
    missing = [c for c in _MEDICAL_PROVIDERS_COLUMNS if c not in table.column_names]
    if missing:
        print(f"  [!] parquet missing columns: {', '.join(missing)}")
        sys.exit(1)
    table = table.select(list(_MEDICAL_PROVIDERS_COLUMNS))

    cols = ", ".join(_MEDICAL_PROVIDERS_COLUMNS)
    copy_sql = (
        f"COPY public.medical_providers ({cols}) FROM STDIN WITH "
        "(FORMAT csv, NULL '')"
    )
    batch_size = 50_000
    loaded = 0
    with conn.cursor() as cur:
        for start in range(0, table.num_rows, batch_size):
            batch = table.slice(start, min(batch_size, table.num_rows - start))
            buf = io.StringIO()
            writer = csv.writer(buf, lineterminator="\n")
            for row in zip(*(batch.column(i).to_pylist() for i in range(batch.num_columns))):
                writer.writerow(["" if v is None else v for v in row])
            buf.seek(0)
            with cur.copy(copy_sql) as copy:
                copy.write(buf.read())
            loaded += batch.num_rows
            print(f"      ... {loaded:,}/{table.num_rows:,} rows")
    conn.commit()
    print(f"  [+] loaded {loaded:,} rows into public.medical_providers")


def create_lakebase_uc_catalog(
    w: WorkspaceClient,
    *,
    catalog_id: str,
    project_id: str,
    postgres_database: str = DEFAULT_LAKEBASE_DATABASE,
) -> None:
    """Register the Lakebase Postgres database as a Unity Catalog catalog."""
    branch = _resolve_lakebase_branch(w, project_id)
    print(
        f"\nRegistering Lakebase database '{postgres_database}' "
        f"as Unity Catalog catalog '{catalog_id}'..."
    )
    try:
        existing = w.catalogs.get(name=catalog_id)
        print(
            f"  [~] catalog '{catalog_id}' already exists "
            f"(type={getattr(existing, 'catalog_type', None) or getattr(existing, 'securable_kind', 'unknown')})"
        )
    except NotFound:
        existing = None
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in ("not found", "does not exist", "404")):
            existing = None
        else:
            print(f"  [!] could not check catalog '{catalog_id}' — {e}")
            sys.exit(1)

    if existing is None:
        try:
            op = w.postgres.create_catalog(
                catalog_id=catalog_id,
                catalog=postgres.Catalog(
                    spec=postgres.CatalogCatalogSpec(
                        postgres_database=postgres_database,
                        branch=branch,
                        create_database_if_missing=False,
                    )
                ),
            )
            op.wait(LroOptions(timeout=timedelta(seconds=300)))
            print(f"  [+] Lakebase UC catalog '{catalog_id}'")
        except Exception as e:
            if _is_already_exists(e):
                print(f"  [~] Lakebase UC catalog '{catalog_id}' already exists")
            else:
                print(f"  [!] create Lakebase UC catalog '{catalog_id}' — {e}")
                sys.exit(1)
    else:
        # Existing catalogs may be an older foreign/federation catalog (e.g. dbo).
        # Warn so operators know Lab 1.1 expects medical_providers.public.* from
        # a Lakebase registration of databricks_postgres.
        print(
            f"  Note: if '{catalog_id}' is not a Lakebase catalog for "
            f"'{postgres_database}.public', delete/rename it and re-run "
            f"with --lakebase-only, or pass --lakebase-catalog <new-name>."
        )

    _grant_privileges(
        w,
        securable_type="catalog",
        full_name=catalog_id,
        privileges=[Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.SELECT],
        description=f"Lakebase catalog '{catalog_id}'",
    )


def setup_lakebase_workshop_source(
    w: WorkspaceClient,
    *,
    display_name: str = DEFAULT_LAKEBASE_NAME,
    pg_version: int = 17,
    postgres_database: str = DEFAULT_LAKEBASE_DATABASE,
    uc_catalog: str = DEFAULT_LAKEBASE_UC_CATALOG,
    parquet_path: Path = DEFAULT_MEDICAL_PROVIDERS_PARQUET,
    skip_if_exists: bool = False,
    force_reload: bool = False,
) -> None:
    """Create Lakebase project, seed medical_providers, register UC catalog."""
    project_id = create_lakebase(
        w, display_name, pg_version, skip_if_exists=skip_if_exists
    )
    branch = _resolve_lakebase_branch(w, project_id)
    endpoint_name, host = _resolve_lakebase_endpoint(w, branch)
    print(f"\nSeeding Lakebase database '{postgres_database}' on {host}...")
    try:
        with _connect_lakebase(
            w,
            endpoint_name=endpoint_name,
            host=host,
            database=postgres_database,
        ) as conn:
            _load_medical_providers_table(
                conn, parquet_path, force_reload=force_reload
            )
    except Exception as e:
        print(f"  [!] Lakebase seed failed — {e}")
        sys.exit(1)

    create_lakebase_uc_catalog(
        w,
        catalog_id=uc_catalog,
        project_id=project_id,
        postgres_database=postgres_database,
    )


def user_catalog_name(email: str) -> str:
    """Match Lab 0 SetCatalogName.py: local-part with dots removed, any
    remaining non-alphanumeric char (e.g. hyphens) turned into ``_`` so the
    result is a valid unquoted UC identifier, then + ``_dev``."""
    local_part = email.split("@", 1)[0]
    slug = re.sub(r"\.", "", local_part).lower()
    slug = re.sub(r"[^a-z0-9]", "_", slug).strip("_")
    if not slug:
        slug = "user"
    return f"{slug}_dev"



def _sql_identifier(name: str, *, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        print(f"Error: invalid {label} {name!r} for SQL (use letters, numbers, _, -)")
        sys.exit(1)
    return name


def _resolve_sql_warehouse_id(w: WorkspaceClient, warehouse_id: str | None) -> str:
    if warehouse_id:
        return warehouse_id
    for warehouse in w.warehouses.list():
        if warehouse.id:
            return warehouse.id
    print("Error: no SQL warehouse found; create one or pass --warehouse-id")
    sys.exit(1)


def _execute_sql_statement(w: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    resp = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    deadline = time.time() + 120
    while resp.status and resp.status.state not in _SQL_TERMINAL_STATES:
        if time.time() >= deadline:
            break
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status is None:
        print("  [!] SQL statement execution: missing status")
        sys.exit(1)
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error.as_dict() if resp.status.error else {}
        print(f"  [!] SQL failed: state={resp.status.state} detail={err}")
        sys.exit(1)


def _create_catalog_if_not_exists_sql(
    w: WorkspaceClient,
    catalog_name: str,
    warehouse_id: str | None,
) -> None:
    safe_name = _sql_identifier(catalog_name, label="catalog name")
    wh_id = _resolve_sql_warehouse_id(w, warehouse_id)
    statement = f"CREATE CATALOG IF NOT EXISTS {safe_name}"
    _execute_sql_statement(w, wh_id, statement)
    print(f"  [+] catalog '{catalog_name}' ({statement})")


_AWS_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)


def _require_aws_credentials() -> None:
    missing = [name for name in _AWS_ENV_VARS if not os.environ.get(name)]
    if not missing:
        return
    print("Error: AWS environment variables are required for Unity Catalog storage setup.")
    print("Set the following in your shell, then re-run this script:\n")
    for name in _AWS_ENV_VARS:
        marker = "  (missing)" if name in missing else ""
        print(f"  export {name}=<value>{marker}")
    sys.exit(1)


def _require_boto3() -> None:
    if boto3 is None:
        print("Error: boto3 is required for AWS provisioning.  pip install boto3")
        sys.exit(1)


def _aws_session_from_env() -> "boto3.Session":
    _require_boto3()
    _require_aws_credentials()
    return boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"],
        region_name=AWS_UC_REGION,
    )


def _catalog_managed_location(external_location_url: str, catalog_name: str) -> str:
    return _normalize_storage_url(f"{external_location_url.rstrip('/')}/{catalog_name}")


def _aws_uc_resource_names(catalog_name: str, aws_account_id: str) -> dict[str, str]:
    safe = re.sub(r"[^a-z0-9-]", "-", catalog_name.lower()).strip("-") or "main"
    bucket = f"databricks-{safe}-uc-{aws_account_id}"[:63].rstrip("-")
    role_name = f"{safe}-unity-catalog-storage"[:64]
    return {
        "bucket_name": bucket,
        "role_name": role_name,
        "role_arn": f"arn:aws:iam::{aws_account_id}:role/{role_name}",
        "storage_credential_name": f"{safe}-storage-credential",
        "external_location_name": f"{safe}-external-location",
    }


def _iam_trust_policy(
    external_id: str,
    *,
    role_arn: str | None = None,
    self_assume_via_root: bool = False,
) -> str:
    external_id = str(external_id).strip()
    statements: list[dict] = [
        {
            "Sid": "UnityCatalogMasterAssume",
            "Effect": "Allow",
            "Principal": {"AWS": UC_AWS_MASTER_ROLE_ARN},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
        },
    ]
    if role_arn:
        if self_assume_via_root:
            account_id = role_arn.split(":")[4]
            principal: dict[str, str] = {"AWS": f"arn:aws:iam::{account_id}:root"}
            extra_condition = {"ArnLike": {"aws:PrincipalArn": role_arn}}
        else:
            principal = {"AWS": role_arn}
            extra_condition = {}
        statements.append(
            {
                "Sid": "RoleSelfAssume",
                "Effect": "Allow",
                "Principal": principal,
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {"sts:ExternalId": external_id},
                    **extra_condition,
                },
            }
        )
    return json.dumps({"Version": "2012-10-17", "Statement": statements})


def _get_iam_role_arn(iam_client, role_name: str) -> str:
    return iam_client.get_role(RoleName=role_name)["Role"]["Arn"]


def _iam_s3_access_policy(bucket_name: str, aws_account_id: str, role_name: str) -> str:
    bucket_arn = f"arn:aws:s3:::{bucket_name}"
    role_arn = f"arn:aws:iam::{aws_account_id}:role/{role_name}"
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                    ],
                    "Resource": [bucket_arn, f"{bucket_arn}/*"],
                },
                {
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Resource": role_arn,
                },
            ],
        }
    )


def _ensure_s3_bucket(s3_client, bucket_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"  [~] S3 bucket '{bucket_name}' (already exists)")
    except ClientError as e:
        if _client_error_code(e) not in ("404", "NoSuchBucket", "NotFound"):
            raise
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": AWS_UC_REGION},
        )
        print(f"  [+] S3 bucket '{bucket_name}' ({AWS_UC_REGION})")


def _ensure_iam_role(iam_client, role_name: str) -> None:
    try:
        iam_client.get_role(RoleName=role_name)
        print(f"  [~] IAM role '{role_name}' (already exists)")
    except ClientError as e:
        if _client_error_code(e) != "NoSuchEntity":
            raise
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=_iam_trust_policy(UC_PLACEHOLDER_EXTERNAL_ID),
            Description="Unity Catalog storage access for State & Local workshop setup",
        )
        print(f"  [+] IAM role '{role_name}'")
        time.sleep(IAM_ROLE_PROPAGATION_SECONDS)


def _ensure_iam_role_policy(
    iam_client, policy_name: str, role_name: str, policy_document: str
) -> None:
    try:
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=policy_document,
        )
        print(f"  [+] IAM inline policy '{policy_name}' on '{role_name}'")
    except ClientError as e:
        print(f"  [!] IAM policy '{policy_name}' on '{role_name}' — {e}")
        sys.exit(1)


def _update_iam_trust_external_id(
    iam_client, role_name: str, external_id: str
) -> None:
    role_arn = _get_iam_role_arn(iam_client, role_name)
    last_error: ClientError | None = None
    for label, use_root in (("role principal", False), ("account root + ArnLike", True)):
        policy = _iam_trust_policy(
            external_id, role_arn=role_arn, self_assume_via_root=use_root
        )
        try:
            iam_client.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=policy,
            )
            print(
                f"  [+] IAM trust policy on '{role_name}' "
                f"(self-assume + external ID, {label})"
            )
            return
        except ClientError as e:
            if _client_error_code(e) != "MalformedPolicyDocument":
                raise
            last_error = e
            print(f"  [~] trust policy variant ({label}) rejected, trying next...")
    print(
        f"  [!] IAM trust policy update failed for '{role_name}' (role_arn={role_arn})."
    )
    if last_error:
        raise last_error


def _storage_credential_external_id(cred) -> str | None:
    if cred.aws_iam_role and cred.aws_iam_role.external_id:
        return cred.aws_iam_role.external_id
    return None


def _get_storage_credential_external_id(w: WorkspaceClient, credential_name: str) -> str | None:
    try:
        cred = w.storage_credentials.get(name=credential_name)
    except NotFound:
        return None
    return _storage_credential_external_id(cred)


def _get_or_create_storage_credential(
    w: WorkspaceClient, credential_name: str, role_arn: str
) -> str:
    external_id = _get_storage_credential_external_id(w, credential_name)
    if external_id:
        print(f"  [~] storage credential '{credential_name}' (already exists)")
        return external_id

    w.storage_credentials.create(
        name=credential_name,
        aws_iam_role=AwsIamRoleRequest(role_arn=role_arn),
        comment="Workshop setup (databricks-sl-workshop)",
        skip_validation=True,
    )
    external_id = _get_storage_credential_external_id(w, credential_name)
    if not external_id:
        print(f"  [!] storage credential '{credential_name}' — no external ID returned")
        sys.exit(1)
    print(f"  [+] storage credential '{credential_name}'")
    return external_id


def _validate_storage_credential(
    w: WorkspaceClient,
    credential_name: str,
    location_url: str,
    *,
    external_location_name: str | None = None,
) -> None:
    try:
        w.storage_credentials.validate(
            storage_credential_name=credential_name,
            url=location_url,
            external_location_name=external_location_name,
        )
        print(f"  [+] validated storage credential '{credential_name}'")
    except Exception as e:
        print(f"  [!] storage credential validation — {e}")
        sys.exit(1)


def _get_or_create_external_location(
    w: WorkspaceClient,
    location_name: str,
    location_url: str,
    credential_name: str,
) -> str:
    try:
        loc = w.external_locations.get(name=location_name)
        if loc.url:
            print(f"  [~] external location '{location_name}' (already exists)")
            return loc.url
    except NotFound:
        pass

    loc = w.external_locations.create(
        name=location_name,
        url=location_url,
        credential_name=credential_name,
        comment="Workshop setup (databricks-sl-workshop)",
        skip_validation=True,
    )
    url = loc.url or location_url
    print(f"  [+] external location '{location_name}' ({url})")
    return url


def provision_aws_uc_storage_for_catalog(
    w: WorkspaceClient, catalog_name: str
) -> AwsUcStorage:
    print(f"\nProvisioning AWS storage for Unity Catalog (catalog: {catalog_name})...")
    session = _aws_session_from_env()
    sts = session.client("sts")
    aws_account_id = sts.get_caller_identity()["Account"]
    names = _aws_uc_resource_names(catalog_name, aws_account_id)
    location_url = _normalize_storage_url(f"s3://{names['bucket_name']}")

    s3 = session.client("s3")
    iam = session.client("iam")

    _ensure_s3_bucket(s3, names["bucket_name"])
    _ensure_iam_role(iam, names["role_name"])
    _ensure_iam_role_policy(
        iam,
        policy_name=f"{names['role_name']}-s3-access",
        role_name=names["role_name"],
        policy_document=_iam_s3_access_policy(
            names["bucket_name"], aws_account_id, names["role_name"]
        ),
    )

    external_id = _get_or_create_storage_credential(
        w, names["storage_credential_name"], names["role_arn"]
    )
    _update_iam_trust_external_id(iam, names["role_name"], external_id)

    url = _get_or_create_external_location(
        w,
        names["external_location_name"],
        location_url,
        names["storage_credential_name"],
    )
    managed_location = _catalog_managed_location(url, catalog_name)
    _validate_storage_credential(
        w,
        names["storage_credential_name"],
        managed_location,
        external_location_name=names["external_location_name"],
    )
    return {
        "external_location_url": url,
        "managed_location": managed_location,
        "storage_credential_name": names["storage_credential_name"],
        "external_location_name": names["external_location_name"],
    }


def _get_catalog_storage_root(w: WorkspaceClient, catalog_name: str) -> str | None:
    try:
        catalog = w.catalogs.get(name=catalog_name)
    except NotFound:
        return None
    for attr in ("storage_root", "storage_location"):
        value = getattr(catalog, attr, None)
        if value:
            return value
    return None


def _alter_catalog_managed_location_sql(
    w: WorkspaceClient,
    catalog_name: str,
    managed_location: str,
    warehouse_id: str | None,
) -> None:
    safe_name = _sql_identifier(catalog_name, label="catalog name")
    escaped_location = managed_location.replace("'", "''")
    statement = (
        f"ALTER CATALOG {safe_name} SET MANAGED LOCATION '{escaped_location}'"
    )
    _execute_sql_statement(w, _resolve_sql_warehouse_id(w, warehouse_id), statement)


def _ensure_catalog_managed_location(
    w: WorkspaceClient,
    catalog_name: str,
    managed_location: str,
    warehouse_id: str | None,
) -> None:
    managed_location = _normalize_storage_url(managed_location)
    current = _get_catalog_storage_root(w, catalog_name)
    if current:
        if _normalize_storage_url(current) == managed_location:
            print(
                f"  [~] catalog '{catalog_name}' managed location "
                f"({managed_location})"
            )
            return
        print(
            f"  [*] Updating catalog '{catalog_name}' managed location:\n"
            f"      was: {current}\n"
            f"      now: {managed_location}"
        )
        _alter_catalog_managed_location_sql(
            w, catalog_name, managed_location, warehouse_id
        )
        print(f"  [+] catalog '{catalog_name}' managed location updated")
        return

    _create_idempotent(
        lambda: w.catalogs.create(name=catalog_name, storage_root=managed_location),
        created_msg=f"catalog '{catalog_name}' (storage_root={managed_location})",
        exists_msg=f"catalog '{catalog_name}' (already exists)",
        error_label=f"catalog '{catalog_name}'",
    )
    if not _get_catalog_storage_root(w, catalog_name):
        _alter_catalog_managed_location_sql(
            w, catalog_name, managed_location, warehouse_id
        )
        print(f"  [+] catalog '{catalog_name}' managed location set")


def _grant_aws_uc_storage_access(
    w: WorkspaceClient,
    storage_credential_name: str,
    external_location_name: str,
) -> None:
    _grant_privileges(
        w,
        securable_type="storage_credential",
        full_name=storage_credential_name,
        privileges=[
            Privilege.CREATE_EXTERNAL_TABLE,
            Privilege.READ_FILES,
            Privilege.WRITE_FILES,
        ],
        description="storage credential",
    )
    _grant_privileges(
        w,
        securable_type="external_location",
        full_name=external_location_name,
        privileges=[Privilege.READ_FILES, Privilege.WRITE_FILES],
        description="external location",
    )


def _has_storage_credentials(w: WorkspaceClient) -> bool:
    try:
        return any(
            True
            for cred in w.storage_credentials.list()
            if not (cred.name and cred.name.startswith("__"))
        )
    except Exception as e:
        print(f"  Note: could not list storage credentials: {e}")
    return False


def _ensure_catalog(
    w: WorkspaceClient,
    catalog_name: str,
    *,
    warehouse_id: str | None,
    aws_storage: AwsUcStorage | None,
) -> None:
    if aws_storage:
        _ensure_catalog_managed_location(
            w,
            catalog_name,
            aws_storage["managed_location"],
            warehouse_id,
        )
    else:
        _create_catalog_if_not_exists_sql(w, catalog_name, warehouse_id)


def _ensure_schemas(
    w: WorkspaceClient,
    catalog_name: str,
    schemas: tuple[str, ...],
) -> None:
    for schema in schemas:
        _create_idempotent(
            lambda s=schema: w.schemas.create(name=s, catalog_name=catalog_name),
            created_msg=f"schema '{catalog_name}.{schema}'",
            exists_msg=f"schema '{catalog_name}.{schema}' (already exists)",
            error_label=f"schema '{catalog_name}.{schema}'",
        )


def _ensure_volume(
    w: WorkspaceClient,
    *,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
) -> str:
    full_volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    _create_idempotent(
        lambda: w.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
        ),
        created_msg=f"volume '{full_volume}'",
        exists_msg=f"volume '{full_volume}' (already exists)",
        error_label=f"volume '{full_volume}'",
    )
    return full_volume


def _upload_tmsis_claims_json(
    w: WorkspaceClient,
    *,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    local_path: Path,
    skip_if_exists: bool,
) -> None:
    """Upload local tmsis_claims.json into the shared Unity Catalog volume."""
    if not local_path.is_file():
        print(f"  [!] TMSIS source file not found: {local_path}")
        print(
            "  Place tmsis_claims.json next to setup_workshop.py "
            "(or pass --tmsis-file) before Lab 1.3."
        )
        sys.exit(1)

    file_name = local_path.name
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}"
    if skip_if_exists:
        try:
            w.files.get_metadata(volume_path)
            print(f"  [~] TMSIS data already present at '{volume_path}'")
            return
        except NotFound:
            pass
        except Exception:
            pass

    size_mb = local_path.stat().st_size / (1024 * 1024)
    print(
        f"  Uploading {local_path.name} ({size_mb:.1f} MiB) -> '{volume_path}' ..."
    )
    try:
        with local_path.open("rb") as fh:
            w.files.upload(volume_path, fh, overwrite=True)
        print(f"  [+] uploaded TMSIS data to '{volume_path}'")
    except Exception as e:
        print(f"  [!] upload TMSIS data to '{volume_path}' — {e}")
        sys.exit(1)


def _upload_workshop_repo_link(w: WorkspaceClient) -> None:
    """Write a text file with the workshop repo URL into the workspace Shared folder."""
    try:
        w.workspace.import_(
            WORKSHOP_REPO_WORKSPACE_PATH,
            content=base64.b64encode(f"{WORKSHOP_REPO_URL}\n".encode()).decode(),
            format=ImportFormat.AUTO,
            overwrite=True,
        )
        print(f"  [+] wrote workshop repo link to '{WORKSHOP_REPO_WORKSPACE_PATH}'")
    except Exception as e:
        print(f"  [!] write workshop repo link to '{WORKSHOP_REPO_WORKSPACE_PATH}' — {e}")


def create_shared_main_catalog(
    w: WorkspaceClient,
    *,
    catalog_name: str = DEFAULT_SHARED_CATALOG,
    schema_name: str = DEFAULT_SHARED_SCHEMA,
    volume_name: str = DEFAULT_TMSIS_VOLUME,
    warehouse_id: str | None = None,
    tmsis_file: Path = DEFAULT_TMSIS_LOCAL_PATH,
    skip_tmsis_upload: bool = False,
) -> None:
    """Create the shared ``main`` catalog, TMSIS volume, and upload claims JSON."""
    print(f"\nCreating shared Unity Catalog resources (catalog: {catalog_name})...")

    aws_storage: AwsUcStorage | None = None
    if _has_storage_credentials(w):
        _require_aws_credentials()
        aws_storage = provision_aws_uc_storage_for_catalog(w, catalog_name)
        _grant_aws_uc_storage_access(
            w,
            aws_storage["storage_credential_name"],
            aws_storage["external_location_name"],
        )

    _ensure_catalog(w, catalog_name, warehouse_id=warehouse_id, aws_storage=aws_storage)
    _ensure_schemas(w, catalog_name, (schema_name,))
    full_volume = _ensure_volume(
        w,
        catalog_name=catalog_name,
        schema_name=schema_name,
        volume_name=volume_name,
    )

    _grant_privileges(
        w,
        securable_type="catalog",
        full_name=catalog_name,
        privileges=[Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.SELECT],
        description=f"shared catalog '{catalog_name}'",
    )
    volume_securable = f"{catalog_name}.{schema_name}.{volume_name}"
    _grant_privileges(
        w,
        securable_type="volume",
        full_name=volume_securable,
        privileges=[Privilege.READ_VOLUME, Privilege.WRITE_VOLUME],
        description=f"shared volume '{volume_securable}'",
    )

    if not skip_tmsis_upload:
        _upload_tmsis_claims_json(
            w,
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_name=volume_name,
            local_path=tmsis_file,
            skip_if_exists=True,
        )
    else:
        print(f"  [~] skipped TMSIS upload for volume '{full_volume}'")

    _upload_workshop_repo_link(w)


def _managed_location_for_catalog(
    aws_storage: AwsUcStorage, catalog_name: str
) -> AwsUcStorage:
    return {
        **aws_storage,
        "managed_location": _catalog_managed_location(
            aws_storage["external_location_url"],
            catalog_name,
        ),
    }


def _set_owner_if_needed(
    *,
    current_owner: str | None,
    desired_owner: str,
    update_fn: Callable[[], None],
    created_msg: str,
    exists_msg: str,
    error_label: str,
) -> None:
    if current_owner and current_owner.lower() == desired_owner.lower():
        print(f"  [~] {exists_msg}")
        return
    try:
        update_fn()
        print(f"  [+] {created_msg}")
    except Exception as e:
        print(f"  [!] {error_label} — {e}")
        sys.exit(1)


def _transfer_user_catalog_ownership(
    w: WorkspaceClient,
    catalog_name: str,
    owner_email: str,
    schemas: tuple[str, ...],
) -> None:
    """Make the participant the Unity Catalog owner of their catalog and schemas."""
    try:
        catalog = w.catalogs.get(name=catalog_name)
    except Exception as e:
        print(f"  [!] could not read catalog '{catalog_name}' to set owner — {e}")
        sys.exit(1)

    _set_owner_if_needed(
        current_owner=catalog.owner,
        desired_owner=owner_email,
        update_fn=lambda: w.catalogs.update(name=catalog_name, owner=owner_email),
        created_msg=f"catalog '{catalog_name}' owner set to '{owner_email}'",
        exists_msg=f"catalog '{catalog_name}' owner already '{owner_email}'",
        error_label=f"set catalog '{catalog_name}' owner to '{owner_email}'",
    )

    for schema in schemas:
        full_name = f"{catalog_name}.{schema}"
        try:
            current = w.schemas.get(full_name=full_name)
        except Exception as e:
            print(f"  [!] could not read schema '{full_name}' to set owner — {e}")
            sys.exit(1)
        _set_owner_if_needed(
            current_owner=current.owner,
            desired_owner=owner_email,
            update_fn=lambda n=full_name: w.schemas.update(
                full_name=n, owner=owner_email
            ),
            created_msg=f"schema '{full_name}' owner set to '{owner_email}'",
            exists_msg=f"schema '{full_name}' owner already '{owner_email}'",
            error_label=f"set schema '{full_name}' owner to '{owner_email}'",
        )


def create_user_catalog(
    w: WorkspaceClient,
    email: str,
    *,
    warehouse_id: str | None,
    aws_storage: AwsUcStorage | None,
) -> None:
    catalog_name = user_catalog_name(email)
    print(f"\nCreating participant catalog '{catalog_name}' for {email}...")
    catalog_storage = (
        _managed_location_for_catalog(aws_storage, catalog_name)
        if aws_storage
        else None
    )
    user_schemas = (DEFAULT_BRONZE_SCHEMA, DEFAULT_SILVER_SCHEMA, DEFAULT_GOLD_SCHEMA)
    _ensure_catalog(
        w,
        catalog_name,
        warehouse_id=warehouse_id,
        aws_storage=catalog_storage,
    )
    _ensure_schemas(
        w,
        catalog_name,
        user_schemas,
    )
    _grant_privileges(
        w,
        securable_type="catalog",
        full_name=catalog_name,
        principal=email,
        privileges=[
            Privilege.ALL_PRIVILEGES,
            Privilege.USE_CATALOG,
            Privilege.USE_SCHEMA,
            Privilege.CREATE_SCHEMA,
            Privilege.CREATE_TABLE,
            Privilege.MODIFY,
            Privilege.SELECT,
        ],
        description=f"participant catalog '{catalog_name}'",
    )
    _transfer_user_catalog_ownership(w, catalog_name, email, user_schemas)


def create_user_catalogs(
    w: WorkspaceClient,
    users_file: str,
    *,
    warehouse_id: str | None,
) -> None:
    path = Path(users_file)
    if not path.exists():
        print(f"Error: users file '{users_file}' not found.")
        sys.exit(1)

    emails = [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if not emails:
        print("No email addresses found in file.")
        return

    aws_storage: AwsUcStorage | None = None
    if _has_storage_credentials(w):
        _require_aws_credentials()
        aws_storage = provision_aws_uc_storage_for_catalog(w, DEFAULT_SHARED_CATALOG)

    print(f"\nCreating {len(emails)} participant catalog(s)...")
    for email in emails:
        create_user_catalog(
            w,
            email,
            warehouse_id=warehouse_id,
            aws_storage=aws_storage,
        )


def _entitlement_values() -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=e) for e in WORKSHOP_ENTITLEMENTS]


def _find_user(w: WorkspaceClient, email: str) -> iam.User | None:
    escaped = email.replace('"', '\\"')
    match = next(iter(w.users.list(filter=f'userName eq "{escaped}"')), None)
    if match is None or not match.id:
        return None
    return w.users.get(id=match.id)


def _ensure_entitlements(w: WorkspaceClient, user: iam.User) -> int:
    current = {e.value for e in (user.entitlements or []) if e.value}
    missing = [e for e in WORKSHOP_ENTITLEMENTS if e not in current]
    if not missing:
        return 0

    operations = [
        iam.Patch(
            op=iam.PatchOp.ADD,
            path="entitlements",
            value=[{"value": e} for e in missing],
        )
    ]
    w.users.patch(
        id=user.id,
        operations=operations,
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )
    print(f"      + entitlements: {', '.join(missing)}")
    return len(missing)


def provision_users(w: WorkspaceClient, users_file: str) -> None:
    path = Path(users_file)
    if not path.exists():
        print(f"Error: users file '{users_file}' not found.")
        sys.exit(1)

    emails = [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if not emails:
        print("No email addresses found in file.")
        return

    print(f"\nProvisioning {len(emails)} user(s)...")

    success = skipped = failed = warnings = 0
    for email in emails:
        display = email.split("@")[0]
        try:
            w.users.create(
                user_name=email,
                display_name=display,
                active=True,
                emails=[iam.ComplexValue(value=email, primary=True)],
                entitlements=_entitlement_values(),
            )
            print(f"  [+] {email}")
            success += 1
        except Exception as e:
            if not _is_already_exists(e):
                print(f"  [!] {email} — {e}")
                failed += 1
                continue
            print(f"  [~] {email} (already exists)")
            skipped += 1
            try:
                existing = _find_user(w, email)
                if existing is None:
                    print(f"      [!] could not look up '{email}' to check entitlements")
                    warnings += 1
                elif _ensure_entitlements(w, existing) == 0:
                    print("      entitlements already granted")
            except Exception as ee:
                print(f"      [!] entitlement check for '{email}' — {ee}")
                warnings += 1

    summary = f"\n  Users: {success} created, {skipped} already existed, {failed} failed."
    if warnings:
        summary += f" ({warnings} entitlement warning(s) — see above.)"
    print(summary)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set up the Databricks State & Local workshop environment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Full setup — Lakebase + shared main catalog + participant catalogs + users:
  python setup_workshop.py --users-file participants.txt

  # Lakebase only (project + medical_providers table + UC catalog):
  python setup_workshop.py --lakebase-only

  # Shared catalog/volume only (uploads workshop_setup/tmsis_claims.json):
  python setup_workshop.py --main-only

  # Participant catalogs only:
  python setup_workshop.py --user-catalogs-only --users-file participants.txt

  # Users only:
  python setup_workshop.py --users-only --users-file participants.txt

  # Infra only — Lakebase + shared + participant catalogs, no user provisioning:
  python setup_workshop.py --infra-only --users-file participants.txt

  # Specify profile:
  python setup_workshop.py --profile sl-workshop --users-file participants.txt

  # UC storage when workspace has storage credentials:
  export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...
  python setup_workshop.py --main-only --profile sl-workshop

Prerequisites:
  - workshop_setup/medical_providers.parquet (Lab 1.1 Lakebase seed data)
  - workshop_setup/tmsis_claims.json (Lab 1.3 volume upload)
  - Provider reviews REST endpoint used by Lab 1.2
        """,
    )

    parser.add_argument(
        "--profile",
        "-p",
        metavar="PROFILE",
        help="Databricks CLI profile from ~/.databrickscfg (prompts if not set)",
    )
    parser.add_argument(
        "--users-file",
        "-u",
        metavar="FILE",
        help="Text file with one user email address per line (# lines ignored)",
    )
    parser.add_argument(
        "--catalog",
        default=DEFAULT_SHARED_CATALOG,
        metavar="NAME",
        help=f"Shared Unity Catalog name (default: {DEFAULT_SHARED_CATALOG})",
    )
    parser.add_argument(
        "--lakebase-name",
        default=DEFAULT_LAKEBASE_NAME,
        metavar="NAME",
        help=f"Lakebase project display name (default: {DEFAULT_LAKEBASE_NAME})",
    )
    parser.add_argument(
        "--lakebase-catalog",
        default=DEFAULT_LAKEBASE_UC_CATALOG,
        metavar="NAME",
        help=(
            "Unity Catalog name for the Lakebase databricks_postgres database "
            f"(default: {DEFAULT_LAKEBASE_UC_CATALOG})"
        ),
    )
    parser.add_argument(
        "--pg-version",
        type=int,
        default=17,
        choices=[16, 17],
        metavar="VER",
        help="PostgreSQL major version for Lakebase (16 or 17, default: 17)",
    )
    parser.add_argument(
        "--medical-providers-file",
        metavar="FILE",
        type=Path,
        default=DEFAULT_MEDICAL_PROVIDERS_PARQUET,
        help=(
            "Parquet file loaded into Lakebase public.medical_providers "
            f"(default: {DEFAULT_MEDICAL_PROVIDERS_PARQUET})"
        ),
    )
    parser.add_argument(
        "--force-reload-providers",
        action="store_true",
        help="Truncate and reload public.medical_providers even if rows exist",
    )
    parser.add_argument(
        "--skip-lakebase-if-exists",
        action="store_true",
        help="If the Lakebase project already exists, skip create/permission updates",
    )
    parser.add_argument(
        "--warehouse-id",
        metavar="ID",
        help=(
            "SQL warehouse ID for CREATE CATALOG IF NOT EXISTS when no external "
            "location is available (default: first warehouse in the workspace)"
        ),
    )
    parser.add_argument(
        "--tmsis-file",
        metavar="FILE",
        type=Path,
        default=DEFAULT_TMSIS_LOCAL_PATH,
        help=(
            "Local TMSIS JSON to upload into the shared volume "
            f"(default: {DEFAULT_TMSIS_LOCAL_PATH})"
        ),
    )
    parser.add_argument(
        "--skip-tmsis-upload",
        action="store_true",
        help="Do not upload tmsis_claims.json into the shared volume",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--lakebase-only",
        action="store_true",
        help="Only create Lakebase project, seed medical_providers, and UC catalog",
    )
    mode.add_argument(
        "--main-only",
        action="store_true",
        help="Only create the shared main catalog, volume, and upload TMSIS JSON",
    )
    mode.add_argument(
        "--user-catalogs-only",
        action="store_true",
        help="Only create participant catalogs from the users file",
    )
    mode.add_argument(
        "--users-only",
        action="store_true",
        help="Only provision users",
    )
    mode.add_argument(
        "--infra-only",
        action="store_true",
        help=(
            "Create Lakebase, shared, and participant Unity Catalog resources, "
            "skipping user provisioning"
        ),
    )

    args = parser.parse_args()

    needs_users_file = not (
        args.main_only or args.users_only or args.lakebase_only
    )
    if needs_users_file and not args.users_file:
        parser.error(
            "--users-file is required unless --lakebase-only, --main-only, "
            "or --users-only is specified"
        )

    profiles = get_profiles()
    profile = select_profile(profiles, args.profile)
    print(f"Profile: {profile}")

    w = WorkspaceClient(profile=profile)
    lakebase_kwargs = {
        "display_name": args.lakebase_name,
        "pg_version": args.pg_version,
        "uc_catalog": args.lakebase_catalog,
        "parquet_path": args.medical_providers_file,
        "skip_if_exists": args.skip_lakebase_if_exists,
        "force_reload": args.force_reload_providers,
    }
    uc_kwargs = {
        "warehouse_id": args.warehouse_id,
        "tmsis_file": args.tmsis_file,
        "skip_tmsis_upload": args.skip_tmsis_upload,
    }

    if args.users_only:
        provision_users(w, args.users_file)
    elif args.lakebase_only:
        setup_lakebase_workshop_source(w, **lakebase_kwargs)
    elif args.main_only:
        create_shared_main_catalog(w, catalog_name=args.catalog, **uc_kwargs)
    elif args.user_catalogs_only:
        create_user_catalogs(w, args.users_file, warehouse_id=args.warehouse_id)
    elif args.infra_only:
        setup_lakebase_workshop_source(w, **lakebase_kwargs)
        create_shared_main_catalog(w, catalog_name=args.catalog, **uc_kwargs)
        create_user_catalogs(w, args.users_file, warehouse_id=args.warehouse_id)
    else:
        setup_lakebase_workshop_source(w, **lakebase_kwargs)
        create_shared_main_catalog(w, catalog_name=args.catalog, **uc_kwargs)
        provision_users(w, args.users_file)
        create_user_catalogs(w, args.users_file, warehouse_id=args.warehouse_id)

    print("\nWorkshop setup complete.")


if __name__ == "__main__":
    main()
