"""Standard Webhooks signatures (https://www.standardwebhooks.com).

Debezium Server's HTTP sink signs with this scheme
(`debezium.sink.http.authentication.type=standard-webhooks`) and so do many
SaaS senders, so one verifier protects every write endpoint of the adapter.
The signed content is `{webhook-id}.{webhook-timestamp}.{body}`, HMAC-SHA256
under the base64 key that follows the `whsec_` prefix of the secret.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import time
from collections.abc import Mapping

ID_HEADER = "webhook-id"
TIMESTAMP_HEADER = "webhook-timestamp"
SIGNATURE_HEADER = "webhook-signature"
SECRET_PREFIX = "whsec_"
TOLERANCE_SECONDS = 300
"""Reject messages signed further than this from now: bounds signature replay."""


class Signer:
    def __init__(self, secret: str) -> None:
        if not secret.startswith(SECRET_PREFIX):
            raise ValueError(f"webhook secret must start with '{SECRET_PREFIX}'")
        try:
            self._key = base64.b64decode(secret[len(SECRET_PREFIX) :], validate=True)
        except binascii.Error as err:
            raise ValueError("webhook secret is not valid base64 after the prefix") from err
        if not self._key:
            raise ValueError("webhook secret is empty")

    def sign(self, msg_id: str, timestamp: int, body: bytes) -> str:
        """The `webhook-signature` header value for one message."""
        content = f"{msg_id}.{timestamp}.".encode() + body
        digest = hmac.new(self._key, content, hashlib.sha256).digest()
        return "v1," + base64.b64encode(digest).decode()

    def headers(self, msg_id: str, body: bytes, now: int | None = None) -> dict[str, str]:
        """All three headers a sender attaches (used by the demo and tests)."""
        timestamp = int(time.time()) if now is None else now
        return {
            ID_HEADER: msg_id,
            TIMESTAMP_HEADER: str(timestamp),
            SIGNATURE_HEADER: self.sign(msg_id, timestamp, body),
        }

    def verify(self, headers: Mapping[str, str], body: bytes, now: int | None = None) -> bool:
        """True when one of the message's `v1` signatures matches and it is fresh."""
        msg_id = headers.get(ID_HEADER)
        stamp = headers.get(TIMESTAMP_HEADER)
        signatures = headers.get(SIGNATURE_HEADER)
        if not msg_id or not stamp or not signatures or not stamp.isdigit():
            return False
        current = int(time.time()) if now is None else now
        if abs(current - int(stamp)) > TOLERANCE_SECONDS:
            return False
        expected = self.sign(msg_id, int(stamp), body)
        return any(hmac.compare_digest(expected, given) for given in signatures.split())
