from __future__ import annotations

import os
from pathlib import Path


def _read_credentials_file() -> dict[str, str]:
    """Read auto-generated credentials from the InputLayer server."""
    candidates = [
        Path(os.environ.get("INPUTLAYER_CREDENTIALS", "")),
        Path(__file__).resolve().parent.parent.parent.parent / ".inputlayer-credentials.toml",
        Path.cwd() / ".inputlayer-credentials.toml",
    ]
    for path in candidates:
        if path.is_file():
            creds: dict[str, str] = {}
            for line in path.read_text().splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    creds[k.strip()] = v.strip().strip('"')
            return creds
    return {}


_creds = _read_credentials_file()

INPUTLAYER_URL = os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws")
INPUTLAYER_USER = os.environ.get("INPUTLAYER_USER", "admin")
INPUTLAYER_PASSWORD = os.environ.get("INPUTLAYER_PASSWORD", _creds.get("admin_password", "admin"))
KG_NAME = os.environ.get("KG_NAME", "reasoning_notebook")
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "http://localhost:5173")

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
LLM_API_KEY = os.environ.get("OPENAI_API_KEY", "lm-studio")
