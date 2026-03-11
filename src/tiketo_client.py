"""Tiketo CMS GraphQL API client."""

import json
import logging

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

    def get_pass_templates(self) -> list[dict]:
        """Fetch all available pass templates."""
        query = """
        query {
            token {
                workspace {
                    passTemplates {
                        id
                        name
                    }
                }
            }
        }
        """
        data = self._execute_query(query)
        return data["token"]["workspace"]["passTemplates"]

    def batch_upsert_passes(
        self,
        template_id: str,
        passes: list[dict],
        batch_size: int,
    ) -> list[dict]:
        """Upsert passes in batches using GraphQL aliases.

        Args:
            template_id: The template ID to use for all passes.
            passes: List of dicts with optional 'id' and 'parameters' dict.
            batch_size: Number of passes per batch request.

        Returns:
            List of results with 'id' and 'share_url' for each pass.
        """
        all_results: list[dict] = []

        for i in range(0, len(passes), batch_size):
            batch = passes[i : i + batch_size]
            batch_num = i // batch_size + 1
            logging.info("Processing batch %d (%d passes)...", batch_num, len(batch))
            results = self._upsert_batch(template_id, batch)
            all_results.extend(results)

        return all_results

    def _upsert_batch(self, template_id: str, batch: list[dict]) -> list[dict]:
        """Upsert a single batch of passes using GraphQL aliases."""
        mutations = []
        for idx, pass_data in enumerate(batch):
            alias = f"p{idx}"
            params_json = json.dumps(pass_data.get("parameters", {}))
            # Escape for GraphQL string embedding
            params_escaped = params_json.replace("\\", "\\\\").replace('"', '\\"')

            input_parts = [f'templateId: "{template_id}"']
            if "id" in pass_data:
                input_parts.append(f'id: "{pass_data["id"]}"')
            input_parts.append(f'parameters: "{params_escaped}"')
            input_str = ", ".join(input_parts)

            mutations.append(f'{alias}: putPass(input: {{{input_str}}}) {{ id share {{ url }} }}')

        mutation = "mutation {\n" + "\n".join(mutations) + "\n}"
        data = self._execute_query(mutation)

        results: list[dict] = []
        for idx in range(len(batch)):
            alias = f"p{idx}"
            entry = data[alias]
            results.append({
                "id": entry["id"],
                "share_url": entry.get("share", {}).get("url", ""),
            })

        return results

    def _execute_query(self, query: str) -> dict:
        """Execute a GraphQL query/mutation and return the data.

        Raises:
            ValueError: For user-facing errors (auth, validation).
            RuntimeError: For unexpected API errors.
        """
        response = self._session.post(
            API_URL,
            json={"query": query},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 401:
            raise ValueError("Authentication failed. Please check your API token.")
        if response.status_code == 403:
            raise ValueError("Access denied. Your token may not have sufficient permissions.")

        if response.status_code != 200:
            raise RuntimeError(
                f"Tiketo API returned HTTP {response.status_code}: {response.text}"
            )

        result = response.json()

        if "errors" in result:
            error_messages = "; ".join(e.get("message", str(e)) for e in result["errors"])
            raise ValueError(f"Tiketo API error: {error_messages}")

        return result.get("data", {})
