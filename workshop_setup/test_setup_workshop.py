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
        workspace.postgres.get_catalog.return_value = SimpleNamespace(
            spec=SimpleNamespace(branch="wrong", postgres_database="databricks_postgres")
        )
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

    def test_user_failures_propagate(self):
        workspace = MagicMock()
        workspace.users.create.side_effect = RuntimeError("permission denied")
        with patch.object(setup, "read_participants", return_value=["a@example.com"]):
            with self.assertRaisesRegex(RuntimeError, "provisioning incomplete"):
                setup.provision_users(workspace, "users.txt")


if __name__ == "__main__":
    unittest.main()
