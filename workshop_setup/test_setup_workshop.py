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
        with self.assertRaises(SystemExit):
            setup._validate_storage_credential(workspace, "credential", "s3://bucket/")

    def test_azure_does_not_use_aws_storage(self):
        workspace = MagicMock()
        workspace.config.is_aws = False
        self.assertFalse(setup._has_storage_credentials(workspace))
        workspace.storage_credentials.list.assert_not_called()

    def test_read_only_endpoint_is_rejected(self):
        workspace = MagicMock()
        workspace.postgres.list_endpoints.return_value = [postgres.Endpoint(
            name="readonly",
            status=postgres.EndpointStatus(
                endpoint_type=postgres.EndpointType.ENDPOINT_TYPE_READ_ONLY
            ),
        )]
        with self.assertRaises(SystemExit):
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
        with self.assertRaises(SystemExit):
            setup._transfer_user_catalog_ownership(
                workspace, "participant_dev", "participant@example.com", ("bronze",)
            )
        workspace.catalogs.update.assert_not_called()

    def test_user_failures_propagate(self):
        workspace = MagicMock()
        workspace.users.create.side_effect = RuntimeError("permission denied")
        with patch.object(setup, "read_participants", return_value=["a@example.com"]):
            with self.assertRaisesRegex(RuntimeError, "provisioning incomplete"):
                setup.provision_users(workspace, "users.txt")

    def test_user_failures_can_be_collected(self):
        workspace = MagicMock()
        workspace.users.create.side_effect = RuntimeError("permission denied")
        with patch.object(setup, "read_participants", return_value=["a@example.com"]):
            self.assertEqual(
                setup.provision_users(workspace, "users.txt", raise_on_failure=False),
                (["a@example.com"], 0),
            )

    def test_failed_users_are_skipped_for_catalogs(self):
        with patch.object(
            setup, "read_participants", return_value=["a@example.com", "b@example.com"]
        ), patch.object(setup, "create_user_catalog") as create:
            setup.create_user_catalogs(
                MagicMock(), "users.txt", warehouse_id="wh",
                storage_checked=True, skip_emails=["A@example.com"],
            )
        self.assertEqual([c.args[1] for c in create.call_args_list], ["b@example.com"])

    def test_default_mode_creates_catalogs_before_reporting_user_failures(self):
        calls = []
        with patch("sys.argv", ["setup", "-p", "x", "-u", "users.txt"]), patch.object(
            setup, "read_participants", return_value=["a@example.com"]
        ), patch.object(setup, "get_profiles", return_value=["x"]), patch.object(
            setup, "WorkspaceClient"
        ), patch.object(setup, "setup_lakebase_workshop_source"), patch.object(
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

    def test_users_file_rules_follow_mode_tuple(self):
        for flag in setup.NO_USERS_FILE_MODES:
            with self.subTest(flag=flag), patch(
                "sys.argv", ["setup", flag, "-u", "users.txt"]
            ), patch("sys.stderr"):
                with self.assertRaises(SystemExit):
                    setup.main()
        for argv in (["setup"], ["setup", "--users-only"], ["setup", "--user-catalogs-only"]):
            with self.subTest(argv=argv), patch("sys.argv", argv), patch("sys.stderr"):
                with self.assertRaises(SystemExit):
                    setup.main()


if __name__ == "__main__":
    unittest.main()
