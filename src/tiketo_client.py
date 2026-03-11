"""Tiketo CMS GraphQL API client."""

from __future__ import annotations

import json
import logging
from typing import Optional

import requests

API_URL = "https://api.cms.tiketo.eu/graphql"
REQUEST_TIMEOUT = 30


class TiketoClient:
    """Client for the Tiketo CMS GraphQL API."""

    def __init__(self, token: str) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"TOKEN {token}",
            "Content-Type": "application/json",
        })

    # ── Queries ───────────────────────────────────────────────────────────

    def get_pass_templates(self) -> list[dict]:
        """Fetch all available pass templates."""
        query = """
        query {
            token {
                workspace {
                    passTemplates { id name }
                }
            }
        }
        """
        data = self._execute(query)
        return data["token"]["workspace"]["passTemplates"]

    def get_passes(self) -> list[dict]:
        """Fetch all passes."""
        query = """
        query {
            token {
                workspace {
                    passes {
                        id templateId memberId parameters
                        expirationDate voided createdAt updatedAt
                        share { url expiration }
                    }
                }
            }
        }
        """
        data = self._execute(query)
        return data["token"]["workspace"]["passes"]

    def get_members(self) -> list[dict]:
        """Fetch all members."""
        query = """
        query {
            token {
                workspace {
                    members {
                        id email phone externalId firstName lastName
                        metadata createdAt updatedAt lastActivityAt
                    }
                }
            }
        }
        """
        data = self._execute(query)
        return data["token"]["workspace"]["members"]

    def get_venues(self) -> list[dict]:
        """Fetch all venues."""
        query = """
        query {
            token {
                workspace {
                    venues {
                        id name description type address metadata
                        createdAt updatedAt
                    }
                }
            }
        }
        """
        data = self._execute(query)
        return data["token"]["workspace"]["venues"]

    def get_organizations(self) -> list[dict]:
        """Fetch all organizations."""
        query = """
        query {
            token {
                workspace {
                    organizations {
                        id name description parentId path depth
                        metadata createdAt updatedAt
                    }
                }
            }
        }
        """
        data = self._execute(query)
        return data["token"]["workspace"]["organizations"]

    def get_campaigns(self) -> list[dict]:
        """Fetch all message campaigns."""
        query = """
        query {
            token {
                workspace {
                    messageCampaigns {
                        id type status messageHeader messageBody
                        dateFrom dateTo templateId
                        totalCount sentCount failedCount
                        metadata createdAt updatedAt completedAt
                    }
                }
            }
        }
        """
        data = self._execute(query)
        return data["token"]["workspace"]["messageCampaigns"]

    # ── Pass mutations ────────────────────────────────────────────────────

    def batch_upsert_passes(
        self,
        passes: list[dict],
        batch_size: int,
    ) -> list[dict]:
        """Upsert passes in batches using GraphQL aliases.

        Each pass dict should have: templateId (required), and optionally
        id, memberId, parameters (dict), expirationDate, voided.
        """
        all_results: list[dict] = []
        for i in range(0, len(passes), batch_size):
            batch = passes[i : i + batch_size]
            logging.info("Processing pass batch %d (%d items)...", i // batch_size + 1, len(batch))
            results = self._upsert_pass_batch(batch)
            all_results.extend(results)
        return all_results

    def _upsert_pass_batch(self, batch: list[dict]) -> list[dict]:
        """Upsert a single batch of passes using GraphQL aliases."""
        mutations = []
        for idx, p in enumerate(batch):
            alias = f"p{idx}"
            input_str = self._build_pass_input(p)
            mutations.append(
                f'{alias}: putPass(input: {{{input_str}}}) {{ id templateId memberId parameters '
                f'expirationDate voided createdAt updatedAt share {{ url }} }}'
            )

        data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")

        results: list[dict] = []
        for idx in range(len(batch)):
            entry = data[f"p{idx}"]
            results.append(self._flatten_pass(entry))
        return results

    def batch_delete_passes(self, pass_ids: list[str], batch_size: int) -> list[dict]:
        """Delete passes in batches."""
        all_results: list[dict] = []
        for i in range(0, len(pass_ids), batch_size):
            batch = pass_ids[i : i + batch_size]
            logging.info("Deleting pass batch %d (%d items)...", i // batch_size + 1, len(batch))
            mutations = []
            for idx, pid in enumerate(batch):
                alias = f"d{idx}"
                mutations.append(f'{alias}: deletePass(id: "{pid}") {{ id }}')
            data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")
            for idx in range(len(batch)):
                all_results.append(data[f"d{idx}"])
        return all_results

    # ── Member mutations ──────────────────────────────────────────────────

    def batch_upsert_members(self, members: list[dict], batch_size: int) -> list[dict]:
        """Upsert members in batches."""
        all_results: list[dict] = []
        for i in range(0, len(members), batch_size):
            batch = members[i : i + batch_size]
            logging.info("Processing member batch %d (%d items)...", i // batch_size + 1, len(batch))
            mutations = []
            for idx, m in enumerate(batch):
                alias = f"m{idx}"
                input_str = self._build_member_input(m)
                mutations.append(
                    f'{alias}: putMember(input: {{{input_str}}}) {{ id email phone externalId '
                    f'firstName lastName metadata createdAt updatedAt lastActivityAt }}'
                )
            data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")
            for idx in range(len(batch)):
                all_results.append(self._flatten_json_fields(data[f"m{idx}"], ["metadata"]))
        return all_results

    def batch_delete_members(self, member_ids: list[str], batch_size: int) -> list[dict]:
        """Delete members in batches."""
        all_results: list[dict] = []
        for i in range(0, len(member_ids), batch_size):
            batch = member_ids[i : i + batch_size]
            logging.info("Deleting member batch %d (%d items)...", i // batch_size + 1, len(batch))
            mutations = []
            for idx, mid in enumerate(batch):
                alias = f"d{idx}"
                mutations.append(f'{alias}: deleteMember(id: "{mid}") {{ id }}')
            data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")
            for idx in range(len(batch)):
                all_results.append(data[f"d{idx}"])
        return all_results

    # ── Venue mutations ───────────────────────────────────────────────────

    def batch_upsert_venues(self, venues: list[dict], batch_size: int) -> list[dict]:
        """Upsert venues in batches."""
        all_results: list[dict] = []
        for i in range(0, len(venues), batch_size):
            batch = venues[i : i + batch_size]
            logging.info("Processing venue batch %d (%d items)...", i // batch_size + 1, len(batch))
            mutations = []
            for idx, v in enumerate(batch):
                alias = f"v{idx}"
                input_str = self._build_venue_input(v)
                mutations.append(
                    f'{alias}: putVenue(input: {{{input_str}}}) {{ id name description type '
                    f'address metadata createdAt updatedAt }}'
                )
            data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")
            for idx in range(len(batch)):
                all_results.append(self._flatten_json_fields(data[f"v{idx}"], ["address", "metadata"]))
        return all_results

    def batch_delete_venues(self, venue_ids: list[str], batch_size: int) -> list[dict]:
        """Delete venues in batches."""
        all_results: list[dict] = []
        for i in range(0, len(venue_ids), batch_size):
            batch = venue_ids[i : i + batch_size]
            logging.info("Deleting venue batch %d (%d items)...", i // batch_size + 1, len(batch))
            mutations = []
            for idx, vid in enumerate(batch):
                alias = f"d{idx}"
                mutations.append(f'{alias}: deleteVenue(id: "{vid}") {{ id }}')
            data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")
            for idx in range(len(batch)):
                all_results.append(data[f"d{idx}"])
        return all_results

    def add_venue_member(self, venue_id: str, member_id: str) -> dict:
        """Add a member to a venue."""
        mutation = f"""
        mutation {{
            addVenueMember(input: {{venueId: "{venue_id}", memberId: "{member_id}"}}) {{
                id name
            }}
        }}
        """
        data = self._execute(mutation)
        return data["addVenueMember"]

    def remove_venue_member(self, venue_id: str, member_id: str) -> dict:
        """Remove a member from a venue."""
        mutation = f"""
        mutation {{
            removeVenueMember(input: {{venueId: "{venue_id}", memberId: "{member_id}"}}) {{
                id name
            }}
        }}
        """
        data = self._execute(mutation)
        return data["removeVenueMember"]

    # ── Organization mutations ────────────────────────────────────────────

    def batch_upsert_organizations(self, orgs: list[dict], batch_size: int) -> list[dict]:
        """Upsert organizations in batches."""
        all_results: list[dict] = []
        for i in range(0, len(orgs), batch_size):
            batch = orgs[i : i + batch_size]
            logging.info("Processing org batch %d (%d items)...", i // batch_size + 1, len(batch))
            mutations = []
            for idx, o in enumerate(batch):
                alias = f"o{idx}"
                input_str = self._build_org_input(o)
                mutations.append(
                    f'{alias}: putOrganization(input: {{{input_str}}}) {{ id name description '
                    f'parentId path depth metadata createdAt updatedAt }}'
                )
            data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")
            for idx in range(len(batch)):
                all_results.append(self._flatten_json_fields(data[f"o{idx}"], ["metadata"]))
        return all_results

    def batch_delete_organizations(self, org_ids: list[str], batch_size: int) -> list[dict]:
        """Delete organizations in batches."""
        all_results: list[dict] = []
        for i in range(0, len(org_ids), batch_size):
            batch = org_ids[i : i + batch_size]
            logging.info("Deleting org batch %d (%d items)...", i // batch_size + 1, len(batch))
            mutations = []
            for idx, oid in enumerate(batch):
                alias = f"d{idx}"
                mutations.append(f'{alias}: deleteOrganization(id: "{oid}") {{ id }}')
            data = self._execute("mutation {\n" + "\n".join(mutations) + "\n}")
            for idx in range(len(batch)):
                all_results.append(data[f"d{idx}"])
        return all_results

    def move_organization(self, org_id: str, new_parent_id: Optional[str]) -> dict:
        """Move organization to a new parent."""
        parent_part = f', newParentId: "{new_parent_id}"' if new_parent_id else ""
        mutation = f"""
        mutation {{
            moveOrganization(input: {{organizationId: "{org_id}"{parent_part}}}) {{
                id name parentId path depth
            }}
        }}
        """
        data = self._execute(mutation)
        return data["moveOrganization"]

    def add_organization_member(self, org_id: str, user_id: str, role: Optional[str] = None) -> dict:
        """Add a user to an organization."""
        role_part = f', role: {role}' if role else ""
        mutation = f"""
        mutation {{
            addOrganizationMember(input: {{organizationId: "{org_id}", userId: "{user_id}"{role_part}}}) {{
                organizationId userId createdAt updatedAt
            }}
        }}
        """
        data = self._execute(mutation)
        return data["addOrganizationMember"]

    def remove_organization_member(self, org_id: str, user_id: str) -> bool:
        """Remove a user from an organization."""
        mutation = f"""
        mutation {{
            removeOrganizationMember(input: {{organizationId: "{org_id}", userId: "{user_id}"}})
        }}
        """
        data = self._execute(mutation)
        return data["removeOrganizationMember"]

    def update_organization_member_role(self, org_id: str, user_id: str, role: str) -> dict:
        """Update a user's role in an organization."""
        mutation = f"""
        mutation {{
            updateOrganizationMemberRole(input: {{organizationId: "{org_id}", userId: "{user_id}", role: {role}}}) {{
                organizationId userId createdAt updatedAt
            }}
        }}
        """
        data = self._execute(mutation)
        return data["updateOrganizationMemberRole"]

    def attach_entity_to_organization(self, org_id: str, entity_type: str, entity_id: str) -> dict:
        """Attach an entity to an organization."""
        mutation = f"""
        mutation {{
            attachEntityToOrganization(input: {{
                organizationId: "{org_id}", entityType: {entity_type}, entityId: "{entity_id}"
            }}) {{
                organizationId
            }}
        }}
        """
        data = self._execute(mutation)
        return data["attachEntityToOrganization"]

    def detach_entity_from_organization(self, org_id: str, entity_type: str, entity_id: str) -> bool:
        """Detach an entity from an organization."""
        mutation = f"""
        mutation {{
            detachEntityFromOrganization(input: {{
                organizationId: "{org_id}", entityType: {entity_type}, entityId: "{entity_id}"
            }})
        }}
        """
        data = self._execute(mutation)
        return data["detachEntityFromOrganization"]

    # ── Campaign mutations ────────────────────────────────────────────────

    def create_passes_campaign(
        self,
        pass_ids: list[str],
        message_body: str,
        message_header: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> dict:
        """Create a campaign targeting specific passes."""
        ids_str = ", ".join(f'"{pid}"' for pid in pass_ids)
        parts = [f'passIds: [{ids_str}]', f'messageBody: "{self._escape(message_body)}"']
        if message_header:
            parts.append(f'messageHeader: "{self._escape(message_header)}"')
        if date_from:
            parts.append(f'dateFrom: "{date_from}"')
        if date_to:
            parts.append(f'dateTo: "{date_to}"')
        input_str = ", ".join(parts)

        mutation = f"""
        mutation {{
            createPassesCampaign(input: {{{input_str}}}) {{
                id type status messageHeader messageBody
                totalCount sentCount failedCount
                createdAt completedAt
            }}
        }}
        """
        data = self._execute(mutation)
        return data["createPassesCampaign"]

    def create_template_campaign(
        self,
        template_ids: list[str],
        message_body: str,
        message_header: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> dict:
        """Create a campaign targeting all passes from templates."""
        ids_str = ", ".join(f'"{tid}"' for tid in template_ids)
        parts = [f'templateIds: [{ids_str}]', f'messageBody: "{self._escape(message_body)}"']
        if message_header:
            parts.append(f'messageHeader: "{self._escape(message_header)}"')
        if date_from:
            parts.append(f'dateFrom: "{date_from}"')
        if date_to:
            parts.append(f'dateTo: "{date_to}"')
        input_str = ", ".join(parts)

        mutation = f"""
        mutation {{
            createTemplateCampaign(input: {{{input_str}}}) {{
                id type status messageHeader messageBody templateId
                totalCount sentCount failedCount
                createdAt completedAt
            }}
        }}
        """
        data = self._execute(mutation)
        return data["createTemplateCampaign"]

    def archive_campaign(self, campaign_id: str) -> dict:
        """Archive a campaign."""
        mutation = f"""
        mutation {{
            archiveCampaign(id: "{campaign_id}") {{
                id status
            }}
        }}
        """
        data = self._execute(mutation)
        return data["archiveCampaign"]

    # ── Input builders ────────────────────────────────────────────────────

    @staticmethod
    def _build_pass_input(p: dict) -> str:
        """Build GraphQL input string for putPass."""
        parts: list[str] = []
        parts.append(f'templateId: "{p["template_id"]}"')
        if p.get("id"):
            parts.append(f'id: "{p["id"]}"')
        if p.get("member_id"):
            parts.append(f'memberId: "{p["member_id"]}"')
        if p.get("expiration_date"):
            parts.append(f'expirationDate: "{p["expiration_date"]}"')
        if "voided" in p:
            parts.append(f'voided: {"true" if p["voided"] else "false"}')
        if p.get("parameters"):
            params_json = json.dumps(p["parameters"])
            params_escaped = params_json.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'parameters: "{params_escaped}"')
        return ", ".join(parts)

    @staticmethod
    def _build_member_input(m: dict) -> str:
        """Build GraphQL input string for putMember."""
        parts: list[str] = []
        if m.get("id"):
            parts.append(f'id: "{m["id"]}"')
        for field in ["email", "phone", "externalId", "external_id",
                      "firstName", "first_name", "lastName", "last_name"]:
            # Support both camelCase and snake_case
            camel = field
            if "_" in field:
                segments = field.split("_")
                camel = segments[0] + "".join(s.capitalize() for s in segments[1:])
            val = m.get(field) or m.get(camel)
            if val:
                parts.append(f'{camel}: "{TiketoClient._escape(val)}"')
        if m.get("metadata"):
            meta = m["metadata"] if isinstance(m["metadata"], str) else json.dumps(m["metadata"])
            meta_escaped = meta.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'metadata: "{meta_escaped}"')
        return ", ".join(parts)

    @staticmethod
    def _build_venue_input(v: dict) -> str:
        """Build GraphQL input string for putVenue."""
        parts: list[str] = []
        if v.get("id"):
            parts.append(f'id: "{v["id"]}"')
        if v.get("name"):
            parts.append(f'name: "{TiketoClient._escape(v["name"])}"')
        if v.get("description"):
            parts.append(f'description: "{TiketoClient._escape(v["description"])}"')
        if v.get("type"):
            parts.append(f'type: "{v["type"]}"')
        for json_field in ["address", "metadata"]:
            if v.get(json_field):
                val = v[json_field] if isinstance(v[json_field], str) else json.dumps(v[json_field])
                val_escaped = val.replace("\\", "\\\\").replace('"', '\\"')
                parts.append(f'{json_field}: "{val_escaped}"')
        return ", ".join(parts)

    @staticmethod
    def _build_org_input(o: dict) -> str:
        """Build GraphQL input string for putOrganization."""
        parts: list[str] = []
        if o.get("id"):
            parts.append(f'id: "{o["id"]}"')
        if o.get("name"):
            parts.append(f'name: "{TiketoClient._escape(o["name"])}"')
        if o.get("description"):
            parts.append(f'description: "{TiketoClient._escape(o["description"])}"')
        if o.get("parent_id") or o.get("parentId"):
            pid = o.get("parent_id") or o.get("parentId")
            parts.append(f'parentId: "{pid}"')
        if o.get("metadata"):
            meta = o["metadata"] if isinstance(o["metadata"], str) else json.dumps(o["metadata"])
            meta_escaped = meta.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'metadata: "{meta_escaped}"')
        return ", ".join(parts)

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _flatten_pass(entry: dict) -> dict:
        """Flatten a Pass response to a flat dict for CSV output."""
        return {
            "id": entry["id"],
            "template_id": entry.get("templateId", ""),
            "member_id": entry.get("memberId", ""),
            "parameters": entry["parameters"] if isinstance(entry.get("parameters"), str)
            else json.dumps(entry["parameters"]) if entry.get("parameters") else "",
            "expiration_date": entry.get("expirationDate", ""),
            "voided": str(entry.get("voided", False)).lower(),
            "share_url": entry.get("share", {}).get("url", "") if entry.get("share") else "",
            "created_at": entry.get("createdAt", ""),
            "updated_at": entry.get("updatedAt", ""),
        }

    @staticmethod
    def _flatten_json_fields(entry: dict, json_fields: list[str]) -> dict:
        """Serialize any JSON/dict fields to strings."""
        result = dict(entry)
        for field in json_fields:
            if field in result and result[field] is not None:
                if isinstance(result[field], (dict, list)):
                    result[field] = json.dumps(result[field])
        return result

    @staticmethod
    def _escape(value: str) -> str:
        """Escape a string for GraphQL inline embedding."""
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    def _execute(self, query: str) -> dict:
        """Execute a GraphQL query/mutation and return the data.

        Raises:
            ValueError: For user-facing errors (auth, validation).
            RuntimeError: For unexpected API errors.
        """
        response = self._session.post(API_URL, json={"query": query}, timeout=REQUEST_TIMEOUT)

        if response.status_code == 401:
            raise ValueError("Authentication failed. Please check your API token.")
        if response.status_code == 403:
            raise ValueError("Access denied. Your token may not have sufficient permissions.")
        if response.status_code != 200:
            raise RuntimeError(f"Tiketo API returned HTTP {response.status_code}: {response.text}")

        result = response.json()
        if "errors" in result:
            error_messages = "; ".join(e.get("message", str(e)) for e in result["errors"])
            raise ValueError(f"Tiketo API error: {error_messages}")

        return result.get("data", {})
