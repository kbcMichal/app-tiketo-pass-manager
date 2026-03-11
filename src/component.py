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
KEY_BATCH_SIZE = "batch_size"

DEFAULT_BATCH_SIZE = 50
MAX_BATCH_SIZE = 100

# ── Extract actions ───────────────────────────────────────────────────────
ACTION_LIST_TEMPLATES = "list_templates"
ACTION_LIST_PASSES = "list_passes"
ACTION_LIST_MEMBERS = "list_members"
ACTION_LIST_VENUES = "list_venues"
ACTION_LIST_ORGANIZATIONS = "list_organizations"
ACTION_LIST_CAMPAIGNS = "list_campaigns"

EXTRACT_ACTIONS = {
    ACTION_LIST_TEMPLATES,
    ACTION_LIST_PASSES,
    ACTION_LIST_MEMBERS,
    ACTION_LIST_VENUES,
    ACTION_LIST_ORGANIZATIONS,
    ACTION_LIST_CAMPAIGNS,
}

# ── Write actions ─────────────────────────────────────────────────────────
ACTION_UPSERT_PASSES = "upsert_passes"
ACTION_DELETE_PASSES = "delete_passes"
ACTION_UPSERT_MEMBERS = "upsert_members"
ACTION_DELETE_MEMBERS = "delete_members"
ACTION_UPSERT_VENUES = "upsert_venues"
ACTION_DELETE_VENUES = "delete_venues"
ACTION_ADD_VENUE_MEMBERS = "add_venue_members"
ACTION_REMOVE_VENUE_MEMBERS = "remove_venue_members"
ACTION_UPSERT_ORGANIZATIONS = "upsert_organizations"
ACTION_DELETE_ORGANIZATIONS = "delete_organizations"
ACTION_MOVE_ORGANIZATIONS = "move_organizations"
ACTION_ADD_ORG_MEMBERS = "add_organization_members"
ACTION_REMOVE_ORG_MEMBERS = "remove_organization_members"
ACTION_UPDATE_ORG_MEMBER_ROLES = "update_organization_member_roles"
ACTION_ATTACH_ENTITIES = "attach_entities_to_organizations"
ACTION_DETACH_ENTITIES = "detach_entities_from_organizations"
ACTION_CREATE_PASSES_CAMPAIGN = "create_passes_campaign"
ACTION_CREATE_TEMPLATE_CAMPAIGN = "create_template_campaign"
ACTION_ARCHIVE_CAMPAIGNS = "archive_campaigns"

ALL_ACTIONS = EXTRACT_ACTIONS | {
    ACTION_UPSERT_PASSES, ACTION_DELETE_PASSES,
    ACTION_UPSERT_MEMBERS, ACTION_DELETE_MEMBERS,
    ACTION_UPSERT_VENUES, ACTION_DELETE_VENUES,
    ACTION_ADD_VENUE_MEMBERS, ACTION_REMOVE_VENUE_MEMBERS,
    ACTION_UPSERT_ORGANIZATIONS, ACTION_DELETE_ORGANIZATIONS,
    ACTION_MOVE_ORGANIZATIONS,
    ACTION_ADD_ORG_MEMBERS, ACTION_REMOVE_ORG_MEMBERS, ACTION_UPDATE_ORG_MEMBER_ROLES,
    ACTION_ATTACH_ENTITIES, ACTION_DETACH_ENTITIES,
    ACTION_CREATE_PASSES_CAMPAIGN, ACTION_CREATE_TEMPLATE_CAMPAIGN, ACTION_ARCHIVE_CAMPAIGNS,
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
            action = params[KEY_ACTION]
            batch_size = params.get(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE)

            if action in EXTRACT_ACTIONS:
                self._run_extract(action)
            else:
                self._run_write(action, params, batch_size)

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
        if action not in ALL_ACTIONS:
            raise ValueError(f"Invalid action: {action}")

        batch_size = params.get(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE)
        if not isinstance(batch_size, int) or batch_size < 1 or batch_size > MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be an integer between 1 and {MAX_BATCH_SIZE}")

        return params

    # ── Extract dispatcher ────────────────────────────────────────────────

    def _run_extract(self, action: str) -> None:
        """Dispatch extract actions."""
        assert self.client is not None
        extract_map = {
            ACTION_LIST_TEMPLATES: ("templates.csv", ["id", "name"], self.client.get_pass_templates),
            ACTION_LIST_PASSES: ("passes.csv", None, self.client.get_passes),
            ACTION_LIST_MEMBERS: ("members.csv", None, self.client.get_members),
            ACTION_LIST_VENUES: ("venues.csv", None, self.client.get_venues),
            ACTION_LIST_ORGANIZATIONS: ("organizations.csv", None, self.client.get_organizations),
            ACTION_LIST_CAMPAIGNS: ("campaigns.csv", None, self.client.get_campaigns),
        }

        filename, fields, fetch_fn = extract_map[action]
        logging.info("Extracting %s...", action)
        records = fetch_fn()
        logging.info("Found %d records.", len(records))

        if action == ACTION_LIST_PASSES:
            records = [TiketoClient._flatten_pass(r) for r in records]

        # Serialize any nested dict/list fields to JSON strings
        records = self._serialize_nested_fields(records)

        if not fields and records:
            fields = list(records[0].keys())

        out_path = Path(self.tables_out_path) / filename
        self._write_csv(out_path, records, fields or [])

        table_def = self.create_out_table_definition(filename, primary_key=["id"], incremental=True)
        self.write_manifest(table_def)

    # ── Write dispatcher ──────────────────────────────────────────────────

    def _run_write(self, action: str, params: dict, batch_size: int) -> None:
        """Dispatch write actions."""
        assert self.client is not None

        # Actions that read from input table
        input_table_actions = {
            ACTION_UPSERT_PASSES, ACTION_DELETE_PASSES,
            ACTION_UPSERT_MEMBERS, ACTION_DELETE_MEMBERS,
            ACTION_UPSERT_VENUES, ACTION_DELETE_VENUES,
            ACTION_ADD_VENUE_MEMBERS, ACTION_REMOVE_VENUE_MEMBERS,
            ACTION_UPSERT_ORGANIZATIONS, ACTION_DELETE_ORGANIZATIONS,
            ACTION_MOVE_ORGANIZATIONS,
            ACTION_ADD_ORG_MEMBERS, ACTION_REMOVE_ORG_MEMBERS, ACTION_UPDATE_ORG_MEMBER_ROLES,
            ACTION_ATTACH_ENTITIES, ACTION_DETACH_ENTITIES,
            ACTION_ARCHIVE_CAMPAIGNS,
        }

        if action in input_table_actions:
            rows = self._read_input_table()
        else:
            rows = []

        if action == ACTION_UPSERT_PASSES:
            self._action_upsert_passes(rows, batch_size)
        elif action == ACTION_DELETE_PASSES:
            self._action_delete_ids(rows, "pass", self.client.batch_delete_passes, batch_size)
        elif action == ACTION_UPSERT_MEMBERS:
            self._action_upsert_members(rows, batch_size)
        elif action == ACTION_DELETE_MEMBERS:
            self._action_delete_ids(rows, "member", self.client.batch_delete_members, batch_size)
        elif action == ACTION_UPSERT_VENUES:
            self._action_upsert_venues(rows, batch_size)
        elif action == ACTION_DELETE_VENUES:
            self._action_delete_ids(rows, "venue", self.client.batch_delete_venues, batch_size)
        elif action == ACTION_ADD_VENUE_MEMBERS:
            self._action_venue_members(rows, add=True)
        elif action == ACTION_REMOVE_VENUE_MEMBERS:
            self._action_venue_members(rows, add=False)
        elif action == ACTION_UPSERT_ORGANIZATIONS:
            self._action_upsert_organizations(rows, batch_size)
        elif action == ACTION_DELETE_ORGANIZATIONS:
            self._action_delete_ids(rows, "organization", self.client.batch_delete_organizations, batch_size)
        elif action == ACTION_MOVE_ORGANIZATIONS:
            self._action_move_organizations(rows)
        elif action == ACTION_ADD_ORG_MEMBERS:
            self._action_org_members(rows, add=True)
        elif action == ACTION_REMOVE_ORG_MEMBERS:
            self._action_org_members(rows, add=False)
        elif action == ACTION_UPDATE_ORG_MEMBER_ROLES:
            self._action_update_org_member_roles(rows)
        elif action == ACTION_ATTACH_ENTITIES:
            self._action_org_entities(rows, attach=True)
        elif action == ACTION_DETACH_ENTITIES:
            self._action_org_entities(rows, attach=False)
        elif action == ACTION_CREATE_PASSES_CAMPAIGN:
            self._action_create_passes_campaign(params)
        elif action == ACTION_CREATE_TEMPLATE_CAMPAIGN:
            self._action_create_template_campaign(params)
        elif action == ACTION_ARCHIVE_CAMPAIGNS:
            self._action_archive_campaigns(rows)

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
        """Upsert passes from input rows. Requires 'template_id' column. All other columns
        except 'id', 'member_id', 'expiration_date', 'voided' become pass parameters."""
        assert self.client is not None
        reserved_fields = {"id", "template_id", "member_id", "expiration_date", "voided"}
        passes: list[dict] = []
        for row in rows:
            if not row.get("template_id"):
                raise ValueError("Input table must have a 'template_id' column for upsert_passes.")
            p: dict = {"template_id": row["template_id"]}
            if row.get("id"):
                p["id"] = row["id"]
            if row.get("member_id"):
                p["member_id"] = row["member_id"]
            if row.get("expiration_date"):
                p["expiration_date"] = row["expiration_date"]
            if row.get("voided"):
                p["voided"] = row["voided"].lower() in ("true", "1", "yes")
            # Remaining columns become parameters
            params = {k: v for k, v in row.items() if k not in reserved_fields and v}
            if params:
                p["parameters"] = params
            passes.append(p)

        logging.info("Upserting %d passes...", len(passes))
        results = self.client.batch_upsert_passes(passes, batch_size)
        self._write_output("passes_result.csv", results, list(results[0].keys()) if results else [])

    def _action_delete_ids(
        self, rows: list[dict], entity_name: str, delete_fn: any, batch_size: int
    ) -> None:
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
                raise ValueError("Input table must have a 'name' column for upsert_venues.")
        logging.info("Upserting %d venues...", len(rows))
        results = self.client.batch_upsert_venues(rows, batch_size)
        self._write_output_with_manifest("venues_result.csv", results)

    def _action_venue_members(self, rows: list[dict], add: bool) -> None:
        """Add or remove members from venues. Input: venue_id, member_id columns."""
        assert self.client is not None
        action_word = "Adding" if add else "Removing"
        fn = self.client.add_venue_member if add else self.client.remove_venue_member
        results: list[dict] = []
        for row in rows:
            if not row.get("venue_id") or not row.get("member_id"):
                raise ValueError("Input table must have 'venue_id' and 'member_id' columns.")
            logging.info("%s member %s to/from venue %s", action_word, row["member_id"], row["venue_id"])
            result = fn(row["venue_id"], row["member_id"])
            results.append({"venue_id": row["venue_id"], "member_id": row["member_id"], "status": "ok"})
        self._write_output("venue_members_result.csv", results, ["venue_id", "member_id", "status"])

    # ── Organization actions ──────────────────────────────────────────────

    def _action_upsert_organizations(self, rows: list[dict], batch_size: int) -> None:
        """Upsert organizations. Requires 'name' column."""
        assert self.client is not None
        for row in rows:
            if not row.get("name"):
                raise ValueError("Input table must have a 'name' column for upsert_organizations.")
        logging.info("Upserting %d organizations...", len(rows))
        results = self.client.batch_upsert_organizations(rows, batch_size)
        self._write_output_with_manifest("organizations_result.csv", results)

    def _action_move_organizations(self, rows: list[dict]) -> None:
        """Move organizations. Input: organization_id, new_parent_id columns."""
        assert self.client is not None
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id"):
                raise ValueError("Input table must have 'organization_id' column.")
            new_parent = row.get("new_parent_id") or None
            logging.info("Moving org %s to parent %s", row["organization_id"], new_parent)
            result = self.client.move_organization(row["organization_id"], new_parent)
            results.append(result)
        self._write_output_with_manifest("organizations_moved.csv", results)

    def _action_org_members(self, rows: list[dict], add: bool) -> None:
        """Add or remove organization members. Input: organization_id, user_id, [role]."""
        assert self.client is not None
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id") or not row.get("user_id"):
                raise ValueError("Input table must have 'organization_id' and 'user_id' columns.")
            if add:
                logging.info("Adding user %s to org %s", row["user_id"], row["organization_id"])
                result = self.client.add_organization_member(
                    row["organization_id"], row["user_id"], row.get("role")
                )
                results.append(result)
            else:
                logging.info("Removing user %s from org %s", row["user_id"], row["organization_id"])
                self.client.remove_organization_member(row["organization_id"], row["user_id"])
                results.append({
                    "organization_id": row["organization_id"],
                    "user_id": row["user_id"],
                    "status": "removed",
                })
        self._write_output_with_manifest("org_members_result.csv", results)

    def _action_update_org_member_roles(self, rows: list[dict]) -> None:
        """Update organization member roles. Input: organization_id, user_id, role."""
        assert self.client is not None
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id") or not row.get("user_id") or not row.get("role"):
                raise ValueError("Input table must have 'organization_id', 'user_id', 'role' columns.")
            logging.info("Updating role for user %s in org %s to %s",
                         row["user_id"], row["organization_id"], row["role"])
            result = self.client.update_organization_member_role(
                row["organization_id"], row["user_id"], row["role"]
            )
            results.append(result)
        self._write_output_with_manifest("org_member_roles_result.csv", results)

    def _action_org_entities(self, rows: list[dict], attach: bool) -> None:
        """Attach or detach entities from organizations."""
        assert self.client is not None
        fn = self.client.attach_entity_to_organization if attach else self.client.detach_entity_from_organization
        action_word = "Attaching" if attach else "Detaching"
        results: list[dict] = []
        for row in rows:
            if not row.get("organization_id") or not row.get("entity_type") or not row.get("entity_id"):
                raise ValueError("Input must have 'organization_id', 'entity_type', 'entity_id' columns.")
            logging.info("%s %s %s to/from org %s",
                         action_word, row["entity_type"], row["entity_id"], row["organization_id"])
            fn(row["organization_id"], row["entity_type"], row["entity_id"])
            results.append({**row, "status": "ok"})
        self._write_output("org_entities_result.csv", results,
                           ["organization_id", "entity_type", "entity_id", "status"])

    # ── Campaign actions ──────────────────────────────────────────────────

    def _action_create_passes_campaign(self, params: dict) -> None:
        """Create a campaign targeting specific passes. Pass IDs from config or input table."""
        assert self.client is not None
        pass_ids = params.get("pass_ids", [])
        if not pass_ids:
            rows = self._read_input_table()
            pass_ids = [r["id"] for r in rows if r.get("id")]
        if not pass_ids:
            raise ValueError("No pass IDs provided. Set 'pass_ids' in config or provide input table with 'id' column.")

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
            raise ValueError("Missing required parameter: template_ids (list of template IDs)")

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
        table_def = self.create_out_table_definition(filename, primary_key=["id"], incremental=True)
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
    def _write_csv(out_path: Path, records: list[dict], fields: list[str]) -> None:
        """Write records to a CSV file."""
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for record in records:
                writer.writerow(record)

    @staticmethod
    def _serialize_nested_fields(records: list[dict]) -> list[dict]:
        """Convert any dict/list values in records to JSON strings for CSV output."""
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
