"""Tiketo Pass Manager component for Keboola."""

from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path
from typing import Optional

from keboola.component import CommonInterface

from tiketo_client import TiketoClient

KEY_TOKEN = "#token"
KEY_ACTION = "action"
KEY_TEMPLATE_ID = "template_id"
KEY_BATCH_SIZE = "batch_size"

ACTION_LIST_TEMPLATES = "list_templates"
ACTION_UPSERT_PASSES = "upsert_passes"

DEFAULT_BATCH_SIZE = 50
MAX_BATCH_SIZE = 100


class Component(CommonInterface):
    def __init__(self) -> None:
        super().__init__()
        self.client: Optional[TiketoClient] = None

    def run(self) -> None:
        """Main execution - orchestrates the component workflow."""
        try:
            params = self._validate_and_get_configuration()
            self.client = TiketoClient(params[KEY_TOKEN])

            action = params[KEY_ACTION]

            if action == ACTION_LIST_TEMPLATES:
                self._extract_templates()
            elif action == ACTION_UPSERT_PASSES:
                batch_size = params.get(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE)
                self._upsert_passes(params[KEY_TEMPLATE_ID], batch_size)
            else:
                raise ValueError(f"Unknown action: {action}")

            logging.info("Component finished successfully.")

        except ValueError as err:
            logging.error(str(err))
            sys.exit(1)
        except Exception as err:
            logging.exception("Unhandled error: %s", err)
            sys.exit(2)

    def _validate_and_get_configuration(self) -> dict:
        """Validate and return component configuration parameters."""
        params = self.configuration.parameters
        if not params.get(KEY_TOKEN):
            raise ValueError("Missing required parameter: #token")
        if not params.get(KEY_ACTION):
            raise ValueError("Missing required parameter: action")

        action = params[KEY_ACTION]
        if action not in (ACTION_LIST_TEMPLATES, ACTION_UPSERT_PASSES):
            raise ValueError(f"Invalid action: {action}. Must be one of: {ACTION_LIST_TEMPLATES}, {ACTION_UPSERT_PASSES}")

        if action == ACTION_UPSERT_PASSES and not params.get(KEY_TEMPLATE_ID):
            raise ValueError("Missing required parameter: template_id (required for upsert_passes action)")

        batch_size = params.get(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE)
        if not isinstance(batch_size, int) or batch_size < 1 or batch_size > MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be an integer between 1 and {MAX_BATCH_SIZE}")

        return params

    def _extract_templates(self) -> None:
        """Extract pass templates from Tiketo and write to output table."""
        assert self.client is not None
        logging.info("Extracting pass templates...")
        templates = self.client.get_pass_templates()
        logging.info("Found %d templates.", len(templates))

        out_path = Path(self.tables_out_path) / "templates.csv"
        self._write_templates_csv(out_path, templates)

        table_def = self.create_out_table_definition(
            "templates.csv",
            primary_key=["id"],
            incremental=True,
        )
        self.write_manifest(table_def)

    def _upsert_passes(self, template_id: str, batch_size: int) -> None:
        """Read input table and upsert passes to Tiketo, write results to output."""
        assert self.client is not None
        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise ValueError("No input table provided. Please map an input table with pass data.")

        input_table = input_tables[0]
        input_path = Path(input_table.full_path)
        logging.info("Reading input table: %s", input_table.name)

        passes = list(self._read_input_passes(input_path))
        logging.info("Found %d passes to upsert.", len(passes))

        results = self.client.batch_upsert_passes(template_id, passes, batch_size)
        logging.info("Successfully upserted %d passes.", len(results))

        out_path = Path(self.tables_out_path) / "passes.csv"
        self._write_passes_csv(out_path, results)

        table_def = self.create_out_table_definition(
            "passes.csv",
            primary_key=["id"],
            incremental=True,
        )
        self.write_manifest(table_def)

    @staticmethod
    def _read_input_passes(input_path: Path) -> list[dict]:
        """Read passes from input CSV file. Each row becomes a pass with parameters from columns."""
        passes = []
        with open(input_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pass_data: dict = {}
                # 'id' column is used as the pass ID (optional)
                if "id" in row and row["id"]:
                    pass_data["id"] = row["id"]
                # All other columns become pass parameters
                parameters = {k: v for k, v in row.items() if k != "id"}
                pass_data["parameters"] = parameters
                passes.append(pass_data)
        return passes

    @staticmethod
    def _write_templates_csv(out_path: Path, templates: list[dict]) -> None:
        """Write templates to CSV output file."""
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"])
            writer.writeheader()
            for template in templates:
                writer.writerow({"id": template["id"], "name": template["name"]})

    @staticmethod
    def _write_passes_csv(out_path: Path, results: list[dict]) -> None:
        """Write upsert results to CSV output file."""
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "share_url"])
            writer.writeheader()
            for result in results:
                writer.writerow({
                    "id": result["id"],
                    "share_url": result.get("share_url", ""),
                })


if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception:
        logging.exception("Component failed")
        sys.exit(2)
