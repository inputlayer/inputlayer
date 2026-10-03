"""Source events -> fact changes -> IQL, without an engine."""

from __future__ import annotations

import pytest

from ingest import debezium, iql, webhooks
from ingest.changes import FactChange, MappingError, Relation, latest_per_key
from ingest.mappings import CUSTOMER, PAYMENT_ISSUE, TABLES, WEBHOOK_EVENTS
from ingest.signing import Signer

SECRET = "whsec_dGVzdC1zZWNyZXQtZm9yLXVuaXQtdGVzdHMtb25seQ=="


def envelope(op: str, lsn: int, before: object = None, after: object = None) -> dict[str, object]:
    source = {"schema": "public", "table": "customers", "lsn": lsn, "snapshot": "false"}
    return {"op": op, "before": before, "after": after, "source": source, "ts_ms": 1}


ACME = {"id": 1, "name": "Acme", "tier": "enterprise"}


def test_debezium_upserts_and_deletes_by_key() -> None:
    created = debezium.change_from_event(envelope("c", 10, after=ACME), TABLES)
    assert created == FactChange(CUSTOMER, (1,), 10, (1, "Acme", "enterprise"))

    # Default replica identity: a delete's `before` holds only the key.
    key_only = {"id": 1, "name": None, "tier": None}
    deleted = debezium.change_from_event(envelope("d", 12, before=key_only), TABLES)
    assert deleted == FactChange(CUSTOMER, (1,), 12, None)


def test_debezium_accepts_lists_schema_wrapped_events_and_tombstones() -> None:
    wrapped = {"schema": {}, "payload": envelope("r", 5, after=ACME)}
    changes = debezium.changes_from_body([wrapped, None, envelope("u", 6, after=ACME)], TABLES)
    assert [c.revision for c in changes] == [5, 6]


@pytest.mark.parametrize(
    "event",
    [
        envelope("t", 1),
        envelope("c", 1, after={"id": 1, "name": None, "tier": "free"}),
        envelope("c", 1, after={"id": "1", "name": "Acme", "tier": "free"}),
        envelope("c", 1, after={"id": True, "name": "Acme", "tier": "free"}),
        envelope("c", -1, after=ACME),
        {**envelope("c", 1, after=ACME), "source": {"schema": "public", "table": "x", "lsn": 1}},
        "not an object",
    ],
)
def test_debezium_rejects_what_it_cannot_map(event: object) -> None:
    with pytest.raises(MappingError):
        debezium.change_from_event(event, TABLES)


def billing_source() -> webhooks.WebhookSource:
    return webhooks.WebhookSource(Signer(SECRET), "sequence", WEBHOOK_EVENTS["billing"])


def test_webhook_upsert_retract_and_unmapped_types() -> None:
    data = {"invoice": "inv_1", "customer_id": 1, "amount_cents": 4200, "extra": "ignored"}
    failed = {"type": "payment.failed", "sequence": 3, "data": data}
    assert webhooks.change_from_event(failed, billing_source()) == FactChange(
        PAYMENT_ISSUE, ("inv_1",), 3, ("inv_1", 1, 4200)
    )
    succeeded = {"type": "payment.succeeded", "sequence": 4, "data": {"invoice": "inv_1"}}
    assert webhooks.change_from_event(succeeded, billing_source()) == FactChange(
        PAYMENT_ISSUE, ("inv_1",), 4, None
    )
    assert webhooks.change_from_event({"type": "customer.created"}, billing_source()) is None


def test_webhook_requires_an_integer_revision() -> None:
    event = {"type": "payment.failed", "sequence": "3", "data": {}}
    with pytest.raises(MappingError):
        webhooks.change_from_event(event, billing_source())


def test_signatures_reject_tampering_staleness_and_other_secrets() -> None:
    signer = Signer(SECRET)
    body = b'{"type":"payment.failed"}'
    headers = signer.headers("evt_1", body, now=1_000)
    assert signer.verify(headers, body, now=1_010)
    assert not signer.verify(headers, body + b" ", now=1_010)
    assert not signer.verify(headers, body, now=1_000 + 301)
    assert not signer.verify({**headers, "webhook-id": "evt_2"}, body, now=1_010)
    assert not signer.verify({}, body, now=1_010)
    other = Signer("whsec_b3RoZXItc2VjcmV0LWZvci11bml0LXRlc3Rz")
    assert not other.verify(headers, body, now=1_010)
    # Rotation: a message may carry several signatures; one match is enough.
    rotated = {**headers, "webhook-signature": "v1,AAAA " + headers["webhook-signature"]}
    assert signer.verify(rotated, body, now=1_010)


def test_latest_per_key_keeps_highest_revision() -> None:
    a1 = FactChange(CUSTOMER, (1,), 5, (1, "A", "free"))
    a2 = FactChange(CUSTOMER, (1,), 7, None)
    b = FactChange(CUSTOMER, (2,), 6, (2, "B", "free"))
    assert latest_per_key([a2, b, a1]) == [a2, b]


def test_apply_program_replaces_by_key_then_records_revisions() -> None:
    upsert = FactChange(CUSTOMER, (1,), 10, (1, 'Ac"me\n', "enterprise"))
    retract = FactChange(PAYMENT_ISSUE, ("inv_1",), 4, None)
    assert iql.apply_program([upsert, retract]).splitlines() == [
        "-customer(1, V1, V2) <- customer(1, V1, V2)",
        '+customer(1, "Ac\\"me\\n", "enterprise")',
        '-payment_issue("inv_1", V1, V2) <- payment_issue("inv_1", V1, V2)',
        '-ingest_revision("customer", "[1]", V2) <- ingest_revision("customer", "[1]", V2)',
        '+ingest_revision("customer", "[1]", 10)',
        '-ingest_revision("payment_issue", "[\\"inv_1\\"]", V2)'
        ' <- ingest_revision("payment_issue", "[\\"inv_1\\"]", V2)',
        '+ingest_revision("payment_issue", "[\\"inv_1\\"]", 4)',
    ]


def test_schema_program_declares_owned_relations_and_revisions() -> None:
    assert iql.schema_program([CUSTOMER]).splitlines() == [
        "+customer(id: int, name: string, tier: string)",
        "+ingest_revision(relation: string, key: string, revision: int)",
    ]


def test_literals() -> None:
    assert [iql.literal(v) for v in (True, -3, 2.5, "a\\b\t")] == [
        "true",
        "-3",
        "2.5",
        '"a\\\\b\\t"',
    ]
    with pytest.raises(ValueError):
        iql.literal(float("nan"))


def test_relation_rejects_non_finite_floats() -> None:
    reading = Relation("reading", (("id", "int"), ("v", "float")), key=("id",))
    assert reading.row_from({"id": 1, "v": 2}) == (1, 2.0)
    with pytest.raises(MappingError):
        reading.row_from({"id": 1, "v": float("nan")})


def test_relation_validates_its_key() -> None:
    with pytest.raises(ValueError):
        Relation("r", (("a", "int"),), key=("b",))
    with pytest.raises(ValueError):
        Relation("r", (("a", "decimal"),), key=("a",))
