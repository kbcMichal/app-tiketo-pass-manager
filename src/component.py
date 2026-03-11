"""Tiketo Pass Manager component for Keboola."""

from __future__ import annotations

import csv
import json
import logging
import sys
from pathlib import Path
from typing import Optional

from keboola.component import CommonInterface

from tiketo_client import TiketoClient

KEY_TOKEN = "#token"
KEY_ACTION = "action"
KEY_ENTITY = "entity"
KEY_OPERATION = "operation"
KEY_BATCH_SIZE = "batch_size"

ACTION_EXTRACT = "extract"
ACTION_WRITE = "write"

DEFAULT_BATCH_SIZE = 50
MAX_BATCH_SIZE = 100

ENTITIES = {"passes", "members", "venues", "organizations", "campaigns"}

OPERATIONS_BY_ENTITY = {
    "passes": {"upsert", "delete"},
    "members": {"upsert", "delete"},
    "venues": {"upsert", "delete", "add_members", "remove_members"},
    "organizations": {
        "upsert", "delete", "move",
        "add_members", "remove_members", "update_member_roles",
        "attach_entities", "detach_entities",
    },
    "campaigns": {"create_passes_campaign", "create_template_campaign", "archive"},
}


class Component(CommonInterface):
    def __init__(self) -> None:
        super().__init__()
        self.client: Optional[TiketoClient] = None

    def run(self) -> None:
        """Main execution - orchestrates the component workflow."""
        try:
            params = self._validate_and_get_configuration()
            self.client = TiketoClient(params[KEY_TOKEN])

            if params[KEY_ACTION] == ACTION_EXTRACT:
                self._run_extract_all()
            else:
                self._run_write(params)

            logging.info("Component finished successfully.")

        except ValueError as err:
            logging.error(str(err))
            sys.exit(1)
        except Exception as err:
            logging.exception("Unhandled error: %s", err)
            sys.exit(2)

    # ── Configuration ─────────────────────────────────────────────────────

    def _validate_and_get_configuration(self) -> dict:
        """Validate and return component configuration parameters."""
        params = self.configuration.parameters
        if not params.get(KEY_TOKEN):
            raise ValueError("Missing required parameter: #token")
        if not params.get(KEY_ACTION):
            raise ValueError("Missing required parameter: action")

        action = params[KEY_ACTION]
        if action not in (ACTION_EXTRACT, ACTION_WRITE):
            raise ValueError(f"Invalid action: {action}. Must be 'extract' or 'write'.")

        if action == ACTION_WRITE:
            entity = params.get(KEY_ENTITY)
            if not entity or entity not in ENTITIES:
                raise ValueError(f"Invalid entity: {entity}. Must be one of: {', '.join(sorted(ENTITIES))}")
            operation = params.get(KEY_OPERATION)
            valid_ops = OPERATIONS_BY_ENTITY[entity]
            if not operation or operation not in valid_ops:
                raise ValueError(
                    f"Invalid operation '{operation}' for entity '{entity}'. "
                    f"Must be one of: {', '.join(sorted(valid_ops))}"
                )

        batch_size = params.get(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE)
        if not isinstance(batch_size, int) or batch_size < 1 or batch_size > MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be an integer between 1 and {MAX_BATCH_SIZE}")

        return params

    # ── Extract all ───────────────────────────────────────────────────────

    def _run_extract_all(self) -> None:
        """Extract all entity types from Tiketo into output tables."""
        assert self.client is not None

        extract_tasks = [
            ("templates", ["id", "name"], self.client.get_pass_templates, None),
            ("passes", None, self.client.get_passes, self._flatten_passes),
            ("members", None, self.client.get_members, None),
            ("venues", None, self.client.get_venues, None),
            ("organizations", None, self.client.get_organizations, None),
            ("campaigns", None, self.client.get_campaigns, None),
        ]

        for name, fields, fetch_fn, transform_fn in extract_tasks:
            logging.info("Extracting %s...", name)
            records = fetch_fn()
            logging.info("Found %d %s.", len(records), name)

            if transform_fn:
                records = transform_fn(records)

            records = self._serialize_nested_fields(records)

            if not fields and records:
                fields = list(records[0].keys())

            if records:
                filename = f"{name}.csv"
                out_path = Path(self.tables_out_path) / filename
                self._write_csv(out_path, records, fields or [])
                table_def = self.create_out_table_definition(
                    filename,
                    destination=f"out.c-tiketo.{name}",
                    primary_key=["id"],
                    incremental=True,
                )
                self.write_manifest(table_def)
            else:
                logging.info("No %s found, skipping output.", name)

    # ── Write dispatcher ──────────────────────────────────────────────────

    def _run_write(self, params: dict) -> None:
        """Dispatch write actions based on entity + operation."""
        assert self.client is not None
        entity = params[KEY_ENTITY]
        operation = params[KEY_OPERATION]
        batch_size = params.get(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE)

        # Campaign creation actions don't always need input table
        needs_input = not (
            entity == "campaigns"
            and operation in ("create_passes_campaign", "create_template_campaign")
            and not self.get_input_tables_definitions()
        )

        rows = self._read_input_table() if needs_input else []

        dispatch = {
            ("passes", "upsert"): lambda: self._action_upsert_passes(rows, batch_size),
            ("passes", "delete"): lambda: self._action_delete(rows, "pass",
                                                              self.client.batch_delete_passes, batch_size),
            ("members", "upsert"): lambda: self._action_upsert_members(rows, batch_size),
            ("members", "delete"): lambda: self._action_delete(rows, "member",
                                                               self.client.batch_delete_members, batch_size),
            ("venues", "upsert"): lambda: self._action_upsert_venues(rows, batch_size),
            ("venues", "delete"): lambda: self._action_delete(rows, "venue",
                                                              self.client.batch_delete_venues, batch_size),
            ("venues", "add_members"): lambda: self._action_venue_members(rows, add=True),
            ("venues", "remove_members"): lambda: self._action_venue_members(rows, add=False),
            ("organizations", "upsert"): lambda: self._action_upsert_organizations(rows, batch_size),
            ("organizations", "delete"): lambda: self._action_delete(
                rows, "organization", self.client.batch_delete_organizations, batch_size),
            ("organizations", "move"): lambda: self._action_move_organizations(rows),
            ("organizations", "add_members"): lambda: self._action_org_members(rows, add=True),
            ("organizations", "remove_members"): lambda: self._action_org_members(rows, add=False),
            ("organizations", "update_member_roles"): lambda: self._action_update_org_member_roles(rows),
            ("organizations", "attach_entities"): lambda: self._action_org_entities(rows, attach=True),
            ("organizations", "detach_entities"): lambda: self._action_org_entities(rows, attach=False),
            ("campaigns", "create_passes_campaign"): lambda: self._action_create_passes_campaign(params, rows),
            ("campaigns", "create_template_campaign"): lambda: self._action_create_template_campaign(params),
            ("campaigns", "archive"): lambda: self._action_archive_campaigns(rows),
        }

        handler = dispatch.get((entity, operation))
        if handler:
            handler()
        else:
            raise ValueError(f"Unknown operation '{operation}' for entity '{entity}'")

    # ── Input reading ─────────────────────────────────────────────────────

    def _read_input_table(self) -> list[dict]:
        """Read the first input table as list of dicts."""
        input_tables = self.get_input_tables_definitions()
        if not input_tables:
            raise ValueError("No input table provided. Please map an input table.")
        input_path = Path(input_tables[0].full_path)
        logging.info("Reading input table: %s", input_tables[0].name)
        with open(input_path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    # ── Pass actions ──────────────────────────────────────────────────────

    def _action_upsert_passes(self, rows: list[dict], batch_size: int) -> None:
        """Upsert passes from input rows."""
        assert self.client is not None
        reserved_fields = {"id", "template_id", "member_id", "expiration_date", "voided"}
        passes: list[dict] = []
        for row in rows:
            if not row.get("template_id"):
                raise ValueError("Input table must have a 'template_id' column.")
            p: dict = {"template_id": row["template_id"]}
            if row.get("id"):
                p["id"] = row["id"]
            if row.get("member_id"):
                p["member_id"] = row["member_id"]
            if row.get("expiration_date"):
                p["expiration_date"] = row["expiration_date"]
            if row.get("voided"):
                p["voided"] = row["voided"].lower() in ("true", "1", "yes")
            params = {k: v for k, v in row.items() if k not in reserved_fields and v}
            if params:
                p["parameters"] = params
            passes.append(p)

        logging.info("Upserting %d passes...", len(passes))
        results = self.client.batch_upsert_passes(passes, batch_size)
        self._write_output_with_manifest("passes_result.csv", results)

    def _action_delete(self, rows: list[dict], entity_name: str,
                       delete_fn: object, batch_size: int) -> None:
        """Generic delete action - reads 'id' column from input."""
        ids = [r["id"] for r in rows if r.get("id")]
        if not ids:
            raise ValueError(f"Input table must have an 'id' column for deleting {entity_name}s.")
        logging.info("Deleting %d %ss...", len(ids), entity_name)
        results = delete_fn(ids, batch_size)
        self._write_output(f"{entity_name}s_deleted.csv", results, ["id"])

    # ── Member actions ────────────────────────────────────────────────────

    def _action_upsert_members(self, rows: list[dict], batch_size: int) -> None:
        """Upsert members from input rows."""
        assert self.client is not None
        logging.info("Upserting %d members...", len(rows))
        results = self.client.batch_upsert_members(rows, batch_size)
        self._write_output_with_manifest("members_result.csv", results)

    # ── Venue actions ─────────────────────────────────────────────────────

    def _action_upsert_venues(self, rows: list[dict], batch_size: int) -> None:
        """Upsert venues from input rows. Requires 'name' column."""
        assert self.client is not None
        for row in rows:
            if not row.get("name"):
                raise ValueError("Input table must have a 'name' column.")
        logging.info("Upserting %d venues...", len(rows))
        results = self.client.batch_upsert_venues(rows, batch_size)
        self._write_output_with_manifest("venues_result.csv", results)

    def _action_venue_members(self, rows: list[dict], add: bool) -> None:
        """Add or remove members from venues."""
        assert self.client is not None
        fn = self.client.add_venue_member if add else self.client.remove_venue_member
        results: list[dict] = []
        for row in rows:
            if not row.get("venue_id") or not row.get("member_id"):
                raise ValueError("Input table must have 'venue_id' and 'member_id' columns.")
            fn(row["venue_id"], row["member_id"])
            results.append({"venue_id": row["venue_id"], "member_id": row["member_id"], "status": "ok"})
        self._write_output("venue_members_result.csv", results, ["venue_id", "member_id", "status"])

    # ── Organization actions ──────────────────────────────────────────────

    def _action_upsert_organizations(self, rows: list[dict], batch_size: int) -> None:
        """Upsert organizations. Requires 'name' column."""
        assert self.client is not None
        for row in rows:
            if not row.get("name"):
                raise ValueError("Input table must have a 'name' column.")
        logging.info("Upserting %d organizations...", len(rows))
        results = self.client.batch_upsert_organizations(rows, batch_size)
        self._write_output_with_manifest("organizations_result.csv", results)

    def _action_move_organizations(self, rows: list[dict]) -> None:
        """Move organizations. Input: organization_id, new_parent_id."""
        assert self.client is not None
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id"):
                raise ValueError("Input table must have 'organization_id' column.")
            new_parent = row.get("new_parent_id") or None
            result = self.client.move_organization(row["organization_id"], new_parent)
            results.append(result)
        self._write_output_with_manifest("organizations_moved.csv", results)

    def _action_org_members(self, rows: list[dict], add: bool) -> None:
        """Add or remove organization members."""
        assert self.client is not None
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id") or not row.get("user_id"):
                raise ValueError("Input table must have 'organization_id' and 'user_id' columns.")
            if add:
                result = self.client.add_organization_member(
                    row["organization_id"], row["user_id"], row.get("role")
                )
                results.append(result)
            else:
                self.client.remove_organization_member(row["organization_id"], row["user_id"])
                results.append({
                    "organization_id": row["organization_id"],
                    "user_id": row["user_id"],
                    "status": "removed",
                })
        self._write_output_with_manifest("org_members_result.csv", results)

    def _action_update_org_member_roles(self, rows: list[dict]) -> None:
        """Update organization member roles."""
        assert self.client is not None
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id") or not row.get("user_id") or not row.get("role"):
                raise ValueError("Input must have 'organization_id', 'user_id', 'role' columns.")
            result = self.client.update_organization_member_role(
                row["organization_id"], row["user_id"], row["role"]
            )
            results.append(result)
        self._write_output_with_manifest("org_member_roles_result.csv", results)

    def _action_org_entities(self, rows: list[dict], attach: bool) -> None:
        """Attach or detach entities from organizations."""
        assert self.client is not None
        fn = (self.client.attach_entity_to_organization if attach
              else self.client.detach_entity_from_organization)
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id") or not row.get("entity_type") or not row.get("entity_id"):
                raise ValueError("Input must have 'organization_id', 'entity_type', 'entity_id' columns.")
            fn(row["organization_id"], row["entity_type"], row["entity_id"])
            results.append({**row, "status": "ok"})
        self._write_output(
            "org_entities_result.csv", results,
            ["organization_id", "entity_type", "entity_id", "status"]
        )

    # ── Campaign actions ──────────────────────────────────────────────────

    def _action_create_passes_campaign(self, params: dict, rows: list[dict]) -> None:
        """Create a campaign targeting specific passes."""
        assert self.client is not None
        pass_ids = params.get("pass_ids", [])
        if not pass_ids and rows:
            pass_ids = [r["id"] for r in rows if r.get("id")]
        if not pass_ids:
            raise ValueError("No pass IDs. Set 'pass_ids' in config or provide input table with 'id' column.")

        message_body = params.get("message_body")
        if not message_body:
            raise ValueError("Missing required parameter: message_body")

        logging.info("Creating passes campaign for %d passes...", len(pass_ids))
        result = self.client.create_passes_campaign(
            pass_ids=pass_ids,
            message_body=message_body,
            message_header=params.get("message_header"),
            date_from=params.get("date_from"),
            date_to=params.get("date_to"),
        )
        self._write_output("campaign_result.csv", [result], list(result.keys()))

    def _action_create_template_campaign(self, params: dict) -> None:
        """Create a campaign targeting all passes from templates."""
        assert self.client is not None
        template_ids = params.get("template_ids", [])
        if not template_ids:
            raise ValueError("Missing required parameter: template_ids")

        message_body = params.get("message_body")
        if not message_body:
            raise ValueError("Missing required parameter: message_body")

        logging.info("Creating template campaign for %d templates...", len(template_ids))
        result = self.client.create_template_campaign(
            template_ids=template_ids,
            message_body=message_body,
            message_header=params.get("message_header"),
            date_from=params.get("date_from"),
            date_to=params.get("date_to"),
        )
        self._write_output("campaign_result.csv", [result], list(result.keys()))

    def _action_archive_campaigns(self, rows: list[dict]) -> None:
        """Archive campaigns. Input: id column."""
        assert self.client is not None
        results: list[dict] = []
        for row in rows:
            if not row.get("id"):
                raise ValueError("Input table must have an 'id' column.")
            logging.info("Archiving campaign %s...", row["id"])
            result = self.client.archive_campaign(row["id"])
            results.append(result)
        self._write_output("campaigns_archived.csv", results, ["id", "status"])

    # ── Output helpers ────────────────────────────────────────────────────

    def _write_output(self, filename: str, records: list[dict], fields: list[str]) -> None:
        """Write records to output CSV and create manifest."""
        records = self._serialize_nested_fields(records)
        out_path = Path(self.tables_out_path) / filename
        self._write_csv(out_path, records, fields)
        table_name = filename.replace(".csv", "")
        table_def = self.create_out_table_definition(
            filename,
            destination=f"out.c-tiketo.{table_name}",
            primary_key=["id"],
            incremental=True,
        )
        self.write_manifest(table_def)

    def _write_output_with_manifest(self, filename: str, records: list[dict]) -> None:
        """Write records to output CSV, auto-detecting fields."""
        if not records:
            logging.warning("No results to write for %s", filename)
            return
        records = self._serialize_nested_fields(records)
        fields = list(records[0].keys())
        self._write_output(filename, records, fields)

    @staticmethod
    def _flatten_passes(records: list[dict]) -> list[dict]:
        """Flatten pass records for CSV output."""
        return [TiketoClient._flatten_pass(r) for r in records]

    @staticmethod
    def _write_csv(out_path: Path, records: list[dict], fields: list[str]) -> None:
        """Write records to a CSV file."""
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for record in records:
                writer.writerow(record)

    @staticmethod
    def _serialize_nested_fields(records: list[dict]) -> list[dict]:
        """Convert any dict/list values in records to JSON strings."""
        result: list[dict] = []
        for record in records:
            row = {}
            for k, v in record.items():
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v)
                elif v is None:
                    row[k] = ""
                else:
                    row[k] = v
            result.append(row)
        return result


if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception:
        logging.exception("Component failed")
        sys.exit(2)
