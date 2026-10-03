"""Tests for inputlayer.auth - API key commands and listing."""

import pytest

from inputlayer.auth import (
    ApiKeyInfo,
    compile_create_api_key,
    compile_expire_api_key,
    parse_api_keys,
)


def test_create_api_key_with_and_without_ttl():
    assert compile_create_api_key("svc") == ".apikey create svc"
    assert compile_create_api_key("svc", "90d") == ".apikey create svc 90d"
    assert compile_create_api_key("svc", "500ms") == ".apikey create svc 500ms"


def test_expire_api_key():
    assert compile_expire_api_key("svc", "24h") == ".apikey expire svc 24h"
    assert compile_expire_api_key("svc", "0s") == ".apikey expire svc 0s"


@pytest.mark.parametrize("ttl", ["", "90", "d", "1y", "-1d", "1.5h", "1d; .user drop x", "1 d"])
def test_invalid_ttls_are_rejected_before_sending(ttl):
    with pytest.raises(ValueError):
        compile_create_api_key("svc", ttl)
    with pytest.raises(ValueError):
        compile_expire_api_key("svc", ttl)


def test_parse_api_keys_maps_columns_by_name():
    columns = ["label", "owner", "created_at", "expires_at", "last_used_at", "status"]
    rows = [
        ["ci", "admin", 1_700_000_000_000, 1_800_000_000_000, None, "active"],
        ["legacy", "bob", None, None, 1_750_000_000_000, "expired"],
    ]
    assert parse_api_keys(columns, rows) == [
        ApiKeyInfo("ci", "admin", 1_700_000_000_000, 1_800_000_000_000, None, "active"),
        ApiKeyInfo("legacy", "bob", None, None, 1_750_000_000_000, "expired"),
    ]
