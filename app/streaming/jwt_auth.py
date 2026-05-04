"""JWT and PAT authentication for Snowpipe Streaming v2.

Supports two auth methods:
  1. Personal Access Token (PAT) - simplest, set snowflake.pat in config
  2. RSA Key-pair -> JWT (direct) - production-grade, used directly as bearer token

For Snowpipe Streaming v2 REST API, the JWT is used directly with
X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT header. No OAuth
token exchange is needed.
"""

import hashlib
import time
import base64
import logging
from pathlib import Path

import jwt
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class SnowflakeAuth:
    """Manages authentication tokens for Snowpipe Streaming v2 REST API."""

    def __init__(self, account: str, user: str, url: str, pat: str = "",
                 private_key_path: str = "", role: str = "ACCOUNTADMIN"):
        self.account = account.upper()
        self.user = user.upper()
        self.url = url.rstrip("/")
        self.pat = pat
        self.private_key_path = private_key_path
        self.role = role
        self._token = None
        self._token_expiry = 0

    def get_token(self) -> str:
        """Return a valid access token, refreshing if expired.

        For PAT: returns the PAT directly.
        For key-pair: generates a JWT (valid 1 hour), caches until near expiry.
        """
        if self.pat:
            return self.pat

        now = time.time()
        if self._token and now < self._token_expiry - 60:
            return self._token

        # Generate JWT and use it directly (no OAuth exchange for streaming API)
        self._token = self._generate_jwt()
        self._token_expiry = now + 3500  # JWT valid for 1 hour, refresh at 58 min
        logger.info("Generated new JWT token (expires in ~58 minutes)")
        return self._token

    def _load_private_key(self):
        """Load RSA private key from file (PEM/DER, with or without passphrase)."""
        key_path = Path(self.private_key_path).expanduser()
        key_data = key_path.read_bytes()

        import os
        passphrase = None
        env_pass = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        if env_pass:
            passphrase = env_pass.encode()

        try:
            private_key = serialization.load_pem_private_key(
                key_data, password=passphrase, backend=default_backend()
            )
        except (ValueError, TypeError):
            private_key = serialization.load_der_private_key(
                key_data, password=passphrase, backend=default_backend()
            )
        return private_key

    def _get_public_key_fingerprint(self, private_key) -> str:
        """Generate SHA-256 fingerprint of the public key (for JWT iss claim)."""
        public_key_der = private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo
        )
        sha256_hash = hashlib.sha256(public_key_der).digest()
        fingerprint = base64.b64encode(sha256_hash).decode("utf-8")
        return f"SHA256:{fingerprint}"

    def _generate_jwt(self) -> str:
        """Generate a JWT for Snowflake key-pair authentication.

        The JWT contains:
          iss: <account>.<user>.<public_key_fingerprint>
          sub: <account>.<user>
          iat: now
          exp: now + 1 hour
        """
        private_key = self._load_private_key()
        fingerprint = self._get_public_key_fingerprint(private_key)

        account_name = self.account.split(".")[0]
        qualified_username = f"{account_name}.{self.user}"

        now = int(time.time())
        payload = {
            "iss": f"{qualified_username}.{fingerprint}",
            "sub": qualified_username,
            "iat": now,
            "exp": now + 3600,
        }

        token = jwt.encode(payload, private_key, algorithm="RS256")
        logger.debug("Generated JWT for %s (fingerprint: %s...)", qualified_username, fingerprint[:20])
        return token

    def get_auth_headers(self) -> dict:
        """Return headers with Bearer token for API calls.

        For PAT: uses KEYPAIR_JWT token type (PATs act like key-pair tokens).
        For key-pair JWT: uses KEYPAIR_JWT token type (direct JWT auth).
        """
        token = self.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
