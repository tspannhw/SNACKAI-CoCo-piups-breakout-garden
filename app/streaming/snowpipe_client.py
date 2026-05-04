"""Snowpipe Streaming v2 REST API client.

Implements channel-based row insertion using the Snowpipe Streaming v2 protocol:
  1. Discover ingest hostname
  2. Open a streaming channel
  3. Append rows as NDJSON batches (with continuation_token)
  4. Close channel on shutdown

Reference: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

from app.streaming.jwt_auth import SnowflakeAuth

logger = logging.getLogger(__name__)

API_V2 = "/v2/streaming"


class SnowpipeStreamingClient:
    """Client for Snowpipe Streaming v2 High Speed ingest."""

    def __init__(self, auth: SnowflakeAuth, database: str, schema: str,
                 pipe: str, channel_name: str):
        self.auth = auth
        self.database = database.upper()
        self.schema = schema.upper()
        self.pipe = pipe.upper()
        self.channel_name = channel_name
        self._ingest_host: Optional[str] = None
        self._channel_id: Optional[str] = None
        self._continuation_token: Optional[str] = None
        self._offset_token: int = 0
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    @property
    def _full_pipe_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.pipe}"

    def connect(self) -> None:
        """Discover ingest host and open a streaming channel."""
        self._discover_ingest_host()
        self._open_channel()
        logger.info("Connected to Snowpipe Streaming: pipe=%s channel=%s",
                    self._full_pipe_name, self._channel_id)

    def _discover_ingest_host(self) -> None:
        """GET /v2/streaming/hostname to find the ingest endpoint."""
        url = f"{self.auth.url}{API_V2}/hostname"
        headers = self.auth.get_auth_headers()

        logger.debug("Discovering ingest host: GET %s", url)
        resp = self._session.get(url, headers=headers, timeout=30)

        if resp.status_code != 200:
            logger.warning("Hostname discovery returned %d: %s", resp.status_code, resp.text[:200])

        resp.raise_for_status()

        # Response may be JSON {"hostname": "..."} or plain text hostname
        body = resp.text.strip()
        try:
            data = resp.json()
            self._ingest_host = data.get("hostname") or data.get("host")
        except ValueError:
            # Plain text response — the body IS the hostname
            if body and "." in body and " " not in body:
                self._ingest_host = body
                logger.info("Hostname returned as plain text: %s", self._ingest_host)
            else:
                logger.error("Unexpected hostname response: %s", body[:200])
                self._ingest_host = None

        if not self._ingest_host:
            from urllib.parse import urlparse
            self._ingest_host = urlparse(self.auth.url).hostname
            logger.warning("Using fallback ingest host: %s", self._ingest_host)
        else:
            logger.info("Discovered ingest host: %s", self._ingest_host)

    def _get_ingest_url(self, path: str) -> str:
        """Build full URL for ingest API calls."""
        host = self._ingest_host or self.auth.url.replace("https://", "").split("/")[0]
        return f"https://{host}{path}"

    def _open_channel(self) -> None:
        """PUT to open a streaming channel on the pipe."""
        channel_suffix = f"{self.channel_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        path = (f"{API_V2}/databases/{self.database}/schemas/{self.schema}"
                f"/pipes/{self.pipe}/channels/{channel_suffix}")
        url = self._get_ingest_url(path)
        headers = self.auth.get_auth_headers()

        logger.debug("Opening channel: PUT %s", url)
        resp = self._session.put(url, headers=headers, json={}, timeout=30)

        if resp.status_code != 200:
            logger.error("Open channel returned %d: %s", resp.status_code, resp.text[:500])

        resp.raise_for_status()

        try:
            result = resp.json()
        except ValueError:
            logger.error("Open channel response not JSON (status=%d, body=%s)",
                         resp.status_code, resp.text[:200])
            result = {}

        self._channel_id = channel_suffix
        self._continuation_token = result.get("next_continuation_token")
        self._offset_token = 0
        logger.info("Opened channel: %s (token: %s)", self._channel_id,
                    self._continuation_token[:20] + "..." if self._continuation_token else "none")

    def append_rows(self, rows: list[dict], _retry: bool = False) -> bool:
        """Append a batch of rows to the streaming channel.

        Args:
            rows: List of dicts where keys match target table column names.
            _retry: Internal flag to prevent infinite retry loops.

        Returns:
            True if successful, False on failure (caller should retry).
        """
        if not rows:
            return True

        if not self._channel_id:
            self.connect()

        path = (f"{API_V2}/data/databases/{self.database}/schemas/{self.schema}"
                f"/pipes/{self.pipe}/channels/{self._channel_id}/rows")

        # Add continuation token as query parameter
        params = {}
        if self._continuation_token:
            params["continuationToken"] = self._continuation_token

        self._offset_token += len(rows)
        params["offsetToken"] = str(self._offset_token)

        url = self._get_ingest_url(path)
        headers = self.auth.get_auth_headers()

        # Build NDJSON payload — sanitize values to avoid NaN/Inf
        ndjson_lines = []
        for row in rows:
            sanitized = {k: (None if isinstance(v, float) and (v != v or v == float('inf') or v == float('-inf')) else v)
                         for k, v in row.items()}
            ndjson_lines.append(json.dumps(sanitized))
        payload = "\n".join(ndjson_lines) + "\n"

        try:
            resp = self._session.post(
                url, headers=headers, data=payload,
                params=params, timeout=30
            )

            if resp.status_code == 401:
                logger.warning("Auth expired, reconnecting channel...")
                self.connect()
                return self.append_rows(rows)

            if resp.status_code == 400:
                error_body = resp.text[:500]
                logger.error("400 Bad Request: %s", error_body)
                if not _retry:
                    # Reopen channel and retry once
                    logger.warning("Reopening channel due to 400 error...")
                    self._open_channel()
                    return self.append_rows(rows, _retry=True)
                return False

            resp.raise_for_status()

            # Update continuation token for next batch
            try:
                result = resp.json()
                self._continuation_token = result.get("next_continuation_token", self._continuation_token)
            except ValueError:
                pass

            logger.debug("Appended %d rows (offset: %d)", len(rows), self._offset_token)
            return True

        except requests.exceptions.RequestException as e:
            logger.error("Failed to append rows: %s", e)
            return False

    def close(self) -> None:
        """Close the streaming channel gracefully."""
        if not self._channel_id:
            return

        try:
            path = (f"{API_V2}/databases/{self.database}/schemas/{self.schema}"
                    f"/pipes/{self.pipe}/channels/{self._channel_id}")
            url = self._get_ingest_url(path)
            headers = self.auth.get_auth_headers()
            self._session.delete(url, headers=headers, timeout=10)
            logger.info("Closed channel: %s", self._channel_id)
        except Exception as e:
            logger.warning("Error closing channel: %s", e)
        finally:
            self._channel_id = None
            self._continuation_token = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
