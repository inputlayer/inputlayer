"""What the adapter ingests: the relations it owns and which source feeds each.

Edit this module to adapt the recipe to your own tables and webhook events.
The adapter declares these relations' schemas on start, so this file is the
single definition of the facts it writes. Relation columns are read from
source fields of the same name.
"""

from __future__ import annotations

from .changes import Relation
from .webhooks import EventMapping

CUSTOMER = Relation(
    name="customer",
    columns=(("id", "int"), ("name", "string"), ("tier", "string")),
    key=("id",),
)

PAYMENT_ISSUE = Relation(
    name="payment_issue",
    columns=(("invoice", "string"), ("customer_id", "int"), ("amount_cents", "int")),
    key=("invoice",),
)

RELATIONS = (CUSTOMER, PAYMENT_ISSUE)

TABLES: dict[str, Relation] = {
    "public.customers": CUSTOMER,
}
"""Postgres `schema.table` -> relation, for Debezium change events."""

WEBHOOK_REVISION_FIELDS: dict[str, str] = {
    "billing": "sequence",
}
"""Webhook source name (the `/webhooks/<source>` path) -> its revision field."""

WEBHOOK_EVENTS: dict[str, dict[str, EventMapping]] = {
    "billing": {
        "payment.failed": EventMapping(PAYMENT_ISSUE),
        "payment.succeeded": EventMapping(PAYMENT_ISSUE, retract=True),
    },
}
"""Webhook source name -> event type -> mapping."""
