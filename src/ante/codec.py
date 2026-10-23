from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Optional

JSON_TYPE = "application/json"
TEXT_TYPE = "text/plain"
BINARY_TYPE = "application/octet-stream"

Parser = Callable[[bytes, Optional[str]], Any]


def encode_payload(payload: Any) -> tuple[bytes, str]:
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload), BINARY_TYPE
    if isinstance(payload, str):
        return payload.encode("utf-8"), TEXT_TYPE
    return (
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        JSON_TYPE,
    )


def decode_payload(
    raw: bytes,
    content_type: str | None,
    parse: Parser | None = None,
) -> Any:
    if parse is not None:
        return parse(raw, content_type)

    kind = (content_type or "").lower()
    if "json" in kind:
        return json.loads(raw)
    if kind == BINARY_TYPE:
        return raw
    if kind:
        return raw.decode("utf-8")

    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return raw.decode("utf-8", "replace")
