"""Local regression checks; no workspace or AWS access required."""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks.sdk.service.catalog import (
    ValidationResult,
    ValidationResultResult,
    ValidateStorageCredentialResponse,
)
from databricks.sdk.service import postgres

import setup_workshop as setup


class SetupTests(unittest.TestCase):
    def participants(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "users.txt"
            path.write_text(text)
            return setup.read_participants(str(path))

    def test_participant_comments_and_duplicates(self):
        self.assertEqual(
            self.participants("  # comment\n\na@example.com\nA@example.com\n"),
            ["a@example.com"],
        )

    def test_participant_catalog_collision(self):
        with self.assertRaisesRegex(ValueError, "share catalog"):
            self.participants("a.b@example.com\nab@other.com\n")

    def test_invalid_participant(self):
        with self.assertRaisesRegex(ValueError, "Invalid email"):
            self.participants("not-an-email\n")

    def test_sql_identifier_is_quoted(self):
        with patch.object(setup, "_execute_sql_statement") as execute:
            setup._create_catalog_if_not_exists_sql(MagicMock(), "my-catalog", "wh")
            execute.assert_called_once_with(
                unittest.mock.ANY, "wh", "CREATE CATALOG IF NOT EXISTS `my-catalog`"
            )

    def test_failed_storage_validation_stops_setup(self):
        workspace = MagicMock()
        workspace.storage_credentials.validate.return_value = ValidateStorageCredentialResponse(
            results=[ValidationResult(result=ValidationResultResult.FAIL, message="denied")]
        )
        with self.assertRaises(RuntimeError):
            setup._validate_storage_credential(workspace, "credential", "s3://bucket/")

    def test_azure_does_not_use_aws_storage(self):
        workspace = MagicMock()
        workspace.config.is_aws = False
        self.assertFalse(setup._has_storage_credentials(workspace))
        workspace.storage_credentials.list.assert_not_called()

    def test_existing_storage_credential_is_read_once_and_role_checked(self):
        for role in ("expected-role", "wrong-role"):
            with self.subTest(role=role):
                workspace = MagicMock()
                workspace.storage_credentials.get.return_value = SimpleNamespace(
                    aws_iam_role=SimpleNamespace(role_arn=role, external_id="external-id")
                )
                if role == "expected-role":
                    self.assertEqual(
                        setup._get_or_create_storage_credential(
                            workspace, "credential", "expected-role"
                        ),
                        "external-id",
                    )
                else:
                    with self.assertRaisesRegex(ValueError, "different IAM role"):
                        setup._get_or_create_storage_credential(
                            workspace, "credential", "expected-role"
                        )
                workspace.storage_credentials.get.assert_called_once_with(name="credential")
                workspace.storage_credentials.create.assert_not_called()

    def test_existing_storage_credential_without_external_id_is_not_recreated(self):
        workspace = MagicMock()
        workspace.storage_credentials.get.return_value = SimpleNamespace(
            aws_iam_role=SimpleNamespace(role_arn="role", external_id=None)
        )
        with self.assertRaisesRegex(RuntimeError, "no external ID"):
            setup._get_or_create_storage_credential(workspace, "credential", "role")
        workspace.storage_credentials.create.assert_not_called()

    def test_read_only_endpoint_is_rejected(self):
        workspace = MagicMock()
        workspace.postgres.list_endpoints.return_value = [postgres.Endpoint(
            name="readonly",
            status=postgres.EndpointStatus(
                endpoint_type=postgres.EndpointType.ENDPOINT_TYPE_READ_ONLY
            ),
        )]
        with self.assertRaises(RuntimeError):
            setup._resolve_lakebase_endpoint(workspace, "branch")

    def test_wrong_catalog_registration_is_rejected(self):
        workspace = MagicMock()
        workspace.postgres.get_catalog.return_value = postgres.Catalog(
            status=postgres.CatalogCatalogStatus(
                branch="wrong", postgres_database="databricks_postgres"
            )
        )
        with patch.object(setup, "_resolve_lakebase_branch", return_value="expected"):
            with self.assertRaisesRegex(ValueError, "not registered"):
                setup.create_lakebase_uc_catalog(
                    workspace, catalog_id="medical_providers", project_id="workshop"
                )
        workspace.grants.update.assert_not_called()

    def test_catalog_registration_uses_observed_status(self):
        for existing in (True, False):
            with self.subTest(existing=existing):
                workspace = MagicMock()
                if not existing:
                    workspace.catalogs.get.side_effect = setup.NotFound("not found")
                workspace.postgres.get_catalog.return_value = postgres.Catalog(
                    status=postgres.CatalogCatalogStatus(
                        branch="expected", postgres_database="databricks_postgres"
                    )
                )
                with patch.object(setup, "_resolve_lakebase_branch", return_value="expected"), patch.object(
                    setup, "_grant_privileges"
                ) as grant:
                    setup.create_lakebase_uc_catalog(
                        workspace, catalog_id="medical_providers", project_id="workshop"
                    )
                grant.assert_called_once()
                self.assertEqual(workspace.postgres.create_catalog.call_count, int(not existing))

    def test_missing_catalog_registration_status_is_rejected(self):
        workspace = MagicMock()
        workspace.postgres.get_catalog.return_value = postgres.Catalog()
        with patch.object(setup, "_resolve_lakebase_branch", return_value="expected"):
            with self.assertRaisesRegex(ValueError, "not registered"):
                setup.create_lakebase_uc_catalog(
                    workspace, catalog_id="medical_providers", project_id="workshop"
                )
        workspace.grants.update.assert_not_called()

    def test_unexpected_catalog_owner_is_preserved(self):
        workspace = MagicMock()
        workspace.catalogs.get.return_value = SimpleNamespace(owner="other@example.com")
        with self.assertRaisesRegex(ValueError, "belongs to"):
            setup.create_user_catalog(
                workspace, "a@example.com", warehouse_id="wh", aws_storage=None
            )
        workspace.catalogs.update.assert_not_called()

    def test_schema_ownership_is_transferred_before_catalog(self):
        workspace = MagicMock()
        workspace.catalogs.get.return_value = SimpleNamespace(owner="setup@example.com")
        workspace.schemas.get.return_value = SimpleNamespace(owner="setup@example.com")
        transferred = []
        workspace.schemas.update.side_effect = lambda **kw: transferred.append(kw["full_name"])
        workspace.catalogs.update.side_effect = lambda **kw: transferred.append(kw["name"])
        setup._transfer_user_catalog_ownership(
            workspace, "participant_dev", "participant@example.com", ("bronze", "silver", "gold")
        )
        self.assertEqual(transferred, [
            "participant_dev.bronze", "participant_dev.silver", "participant_dev.gold",
            "participant_dev",
        ])

    def test_setup_user_keeps_catalog_access_before_transfer(self):
        workspace = MagicMock()
        workspace.current_user.me.return_value = SimpleNamespace(user_name="setup@example.com")
        workspace.catalogs.get.return_value = SimpleNamespace(owner="setup@example.com")
        workspace.schemas.get.return_value = SimpleNamespace(owner="participant@example.com")
        calls = []
        workspace.grants.update.side_effect = lambda **kw: calls.append(("grant", kw["changes"][0]))
        workspace.catalogs.update.side_effect = lambda **kw: calls.append(("owner", kw["owner"]))
        setup._transfer_user_catalog_ownership(
            workspace, "participant_dev", "participant@example.com", ("bronze",)
        )
        self.assertEqual([c[0] for c in calls], ["grant", "owner"])
        change = calls[0][1]
        self.assertEqual(change.principal, "setup@example.com")
        self.assertEqual(set(change.add), {setup.Privilege.BROWSE, setup.Privilege.USE_CATALOG})

    def test_setup_user_regains_access_to_transferred_catalog(self):
        workspace = MagicMock()
        workspace.current_user.me.return_value = SimpleNamespace(user_name="setup@example.com")
        workspace.catalogs.get.return_value = SimpleNamespace(owner="a@example.com")
        calls = []
        workspace.grants.update.side_effect = lambda **kw: calls.append(("grant", kw["changes"][0]))
        with patch.object(setup, "_ensure_catalog"), patch.object(
            setup, "_ensure_schemas", side_effect=lambda *a, **k: calls.append(("schemas", None))
        ), patch.object(setup, "_transfer_user_catalog_ownership"):
            setup.create_user_catalog(
                workspace, "a@example.com", warehouse_id="wh", aws_storage=None
            )
        self.assertEqual(calls[0][0], "grant")
        self.assertEqual(calls[1][0], "schemas")
        change = calls[0][1]
        self.assertEqual(change.principal, "setup@example.com")
        self.assertIn(setup.Privilege.CREATE_SCHEMA, change.add)
        self.assertIn(setup.Privilege.USE_CATALOG, change.add)

    def test_unreadable_existing_catalog_is_skipped(self):
        workspace = MagicMock()
        workspace.catalogs.get.side_effect = setup.PermissionDenied("no USE CATALOG")
        with patch.object(setup, "_ensure_catalog") as ensure:
            setup.create_user_catalog(
                workspace, "a@example.com", warehouse_id="wh", aws_storage=None
            )
        ensure.assert_not_called()

    def test_schema_transfer_failure_preserves_catalog_ownership(self):
        workspace = MagicMock()
        workspace.schemas.update.side_effect = RuntimeError("denied")
        with self.assertRaises(RuntimeError):
            setup._transfer_user_catalog_ownership(
                workspace, "participant_dev", "participant@example.com", ("bronze",)
            )
        workspace.catalogs.update.assert_not_called()

    def test_user_failures_propagate(self):
        workspace = MagicMock()
        workspace.users.create.side_effect = RuntimeError("permission denied")
        with self.assertRaisesRegex(RuntimeError, "provisioning incomplete"):
            setup.provision_users(workspace, ["a@example.com"])

    def test_user_failures_can_be_collected(self):
        workspace = MagicMock()
        workspace.users.create.side_effect = RuntimeError("permission denied")
        self.assertEqual(
            setup.provision_users(workspace, ["a@example.com"], raise_on_failure=False),
            (["a@example.com"], 0),
        )

    def test_failed_users_are_skipped_for_catalogs(self):
        with patch.object(setup, "create_user_catalog") as create:
            setup.create_user_catalogs(
                MagicMock(), ["a@example.com", "b@example.com"], warehouse_id="wh",
                storage_checked=True, skip_emails=["A@example.com"],
            )
        self.assertEqual([c.args[1] for c in create.call_args_list], ["b@example.com"])

    def test_catalog_failure_skips_user_and_continues(self):
        attempted = []

        def create(w, email, **kw):
            attempted.append(email)
            if email == "a@example.com":
                raise RuntimeError("schema setup failed")
            if email == "b@example.com":
                raise ValueError("belongs to someone else")

        emails = ["a@example.com", "b@example.com", "c@example.com"]
        with patch.object(setup, "create_user_catalog", side_effect=create):
            with self.assertRaisesRegex(RuntimeError, "2 user"):
                setup.create_user_catalogs(
                    MagicMock(), emails, warehouse_id="wh", storage_checked=True
                )
            self.assertEqual(attempted, emails)
            self.assertEqual(
                setup.create_user_catalogs(
                    MagicMock(), emails, warehouse_id="wh",
                    storage_checked=True, raise_on_failure=False,
                ),
                ["a@example.com", "b@example.com"],
            )

    def test_default_mode_creates_catalogs_before_reporting_user_failures(self):
        calls = []
        with patch("sys.argv", ["setup", "-p", "x", "-u", "users.txt"]), patch.object(
            setup, "read_participants", return_value=["a@example.com"]
        ), patch.object(setup, "get_profiles", return_value=["x"]), patch.object(
            setup, "WorkspaceClient"
        ), patch.object(setup, "ensure_metastore_admin"), patch.object(
            setup, "setup_lakebase_workshop_source"
        ), patch.object(
            setup, "create_shared_main_catalog", return_value=None
        ), patch.object(
            setup, "provision_users", return_value=(["a@example.com"], 0)
        ), patch.object(
            setup, "create_user_catalogs",
            side_effect=lambda *a, **kw: calls.append(kw["skip_emails"]),
        ):
            with self.assertRaisesRegex(RuntimeError, "provisioning incomplete"):
                setup.main()
        self.assertEqual(calls, [["a@example.com"]])

    def test_users_only_creates_catalogs_and_checks_metastore_admin(self):
        calls = []
        with patch("sys.argv", ["setup", "-p", "x", "--users-only", "-u", "users.txt"]), \
                patch.object(setup, "read_participants", return_value=["a@example.com"]), \
                patch.object(setup, "get_profiles", return_value=["x"]), \
                patch.object(setup, "WorkspaceClient"), \
                patch.object(setup, "ensure_metastore_admin") as admin, \
                patch.object(setup, "setup_lakebase_workshop_source") as lakebase, \
                patch.object(setup, "create_shared_main_catalog") as shared, \
                patch.object(setup, "provision_users", return_value=([], 0)), \
                patch.object(
                    setup, "create_user_catalogs",
                    side_effect=lambda *a, **kw: calls.append(kw) or [],
                ):
            setup.main()
        admin.assert_called_once()
        lakebase.assert_not_called()
        shared.assert_not_called()
        self.assertEqual(len(calls), 1)
        self.assertFalse(calls[0]["storage_checked"])

    def test_users_file_rules_follow_mode_tuple(self):
        for flag in setup.NO_USERS_FILE_MODES:
            with self.subTest(flag=flag), patch(
                "sys.argv", ["setup", flag, "-u", "users.txt"]
            ), patch("sys.stderr"):
                with self.assertRaises(SystemExit):
                    setup.main()

        for argv in (["setup"], ["setup", "--users-only"]):
            with self.subTest(argv=argv), patch("sys.argv", argv), patch("sys.stderr"):
                with self.assertRaises(SystemExit):
                    setup.main()

    def test_modes_preserve_stage_order_and_read_participants_once(self):
        for flag, expected in (
            (None, ["lakebase", "shared", "users"]),
            ("--lakebase-only", ["lakebase"]),
            ("--main-only", ["shared"]),
            ("--infra-only", ["lakebase", "shared"]),
            ("--users-only", ["users"]),
        ):
            with self.subTest(mode=flag):
                argv = ["setup", "-p", "x"]
                if flag:
                    argv.append(flag)
                has_users = "users" in expected
                if has_users:
                    argv.extend(["-u", "users.txt"])
                calls = []
                emails = ["a@example.com"]
                storage = {"managed_location": "s3://bucket/main/"}

                def shared(*args, **kwargs):
                    calls.append("shared")
                    return storage

                with patch("sys.argv", argv), patch.object(
                    setup, "read_participants", return_value=emails
                ) as read, patch.object(
                    setup, "get_profiles", return_value=["x"]
                ), patch.object(setup, "WorkspaceClient"), patch.object(
                    setup, "ensure_metastore_admin"
                ), patch.object(
                    setup, "setup_lakebase_workshop_source",
                    side_effect=lambda *a, **k: calls.append("lakebase"),
                ), patch.object(
                    setup, "create_shared_main_catalog", side_effect=shared
                ), patch.object(
                    setup, "provision_users",
                    side_effect=lambda *a, **k: calls.append("users") or ([], 0),
                ) as users, patch.object(
                    setup, "create_user_catalogs", return_value=[]
                ) as catalogs:
                    setup.main()
                self.assertEqual(calls, expected)
                self.assertEqual(read.call_count, int(has_users))
                if has_users:
                    self.assertIs(users.call_args.args[1], emails)
                    self.assertIs(catalogs.call_args.args[1], emails)
                    self.assertEqual(catalogs.call_args.kwargs["storage_checked"], "shared" in expected)
                    self.assertIs(
                        catalogs.call_args.kwargs["aws_storage"],
                        storage if "shared" in expected else None,
                    )

    def metastore_workspace(self, owner, user="me@example.com", groups=()):
        workspace = MagicMock()
        workspace.metastores.current.return_value = SimpleNamespace(metastore_id="ms-1")
        workspace.metastores.get.return_value = SimpleNamespace(
            metastore_id="ms-1", name="metastore", owner=owner
        )
        workspace.current_user.me.return_value = SimpleNamespace(
            user_name=user, groups=[SimpleNamespace(display=g) for g in groups]
        )
        return workspace

    def test_metastore_admin_direct_or_via_group(self):
        for owner, groups in (("ME@example.com", ()), ("admins", ("admins",))):
            workspace = self.metastore_workspace(owner, groups=groups)
            setup.ensure_metastore_admin(workspace)
            workspace.metastores.update.assert_not_called()

    def test_metastore_admin_prompt_yes_changes_owner(self):
        workspace = self.metastore_workspace("someone@example.com")
        with patch.object(setup.sys.stdin, "isatty", return_value=True), \
                patch("builtins.input", return_value="y"):
            setup.ensure_metastore_admin(workspace)
        workspace.metastores.update.assert_called_once_with(
            id="ms-1", owner="me@example.com"
        )

    def test_metastore_admin_prompt_no_leaves_owner(self):
        workspace = self.metastore_workspace("someone@example.com")
        with patch.object(setup.sys.stdin, "isatty", return_value=True), \
                patch("builtins.input", return_value=""):
            setup.ensure_metastore_admin(workspace)
        workspace.metastores.update.assert_not_called()

    def test_metastore_admin_non_interactive_requires_flag(self):
        workspace = self.metastore_workspace("someone@example.com")
        with patch.object(setup.sys.stdin, "isatty", return_value=False):
            with self.assertRaises(RuntimeError):
                setup.ensure_metastore_admin(workspace)
            setup.ensure_metastore_admin(workspace, assume_yes=True)
        workspace.metastores.update.assert_called_once_with(
            id="ms-1", owner="me@example.com"
        )

    def test_metastore_permission_denied_reaches_prompt(self):
        for answer in ("y", "n"):
            with self.subTest(answer=answer):
                workspace = self.metastore_workspace(None)
                workspace.metastores.get.side_effect = setup.PermissionDenied("denied")
                with patch.object(setup.sys.stdin, "isatty", return_value=True), \
                        patch("builtins.input", return_value=answer) as prompt:
                    setup.ensure_metastore_admin(workspace)
                self.assertIn("unknown", prompt.call_args.args[0])
                if answer == "y":
                    workspace.metastores.update.assert_called_once_with(
                        id="ms-1", owner="me@example.com"
                    )
                else:
                    workspace.metastores.update.assert_not_called()

    def test_metastore_permission_denied_allows_explicit_flag(self):
        workspace = self.metastore_workspace(None)
        workspace.metastores.get.side_effect = setup.PermissionDenied("denied")
        with patch.object(setup.sys.stdin, "isatty", return_value=False):
            setup.ensure_metastore_admin(workspace, assume_yes=True)
        workspace.metastores.update.assert_called_once_with(
            id="ms-1", owner="me@example.com"
        )


if __name__ == "__main__":
    unittest.main()
