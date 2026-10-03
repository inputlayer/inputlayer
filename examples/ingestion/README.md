# Ingestion recipe: Postgres CDC and webhooks to InputLayer facts

```bash
./demo.sh
```

This starts Postgres, Debezium Server, the adapter and InputLayer, then runs a self-checking
demo: row changes and billing webhooks update a derived answer, and a watching client
receives every change. Full walkthrough, guarantees and production notes:
[docs/content/docs/guides/ingestion.mdx](../../docs/content/docs/guides/ingestion.mdx).

| Path | What it is |
|---|---|
| `docker-compose.yml` | the stack; loopback ports and generated credentials in `.env` |
| `debezium/application.properties` | Debezium Server: Postgres `pgoutput` to the adapter's `/cdc` |
| `postgres/init.sql` | the demo source table |
| `adapter/ingest/mappings.py` | which tables and webhook events feed which relations: edit this |
| `adapter/ingest/` | the adapter (`server.py`), the watching client (`watch.py`), the demo (`demo.py`) |
| `adapter/tests/` | unit tests: `cd adapter && pip install -e '.[dev]' && pytest` |

The script generates fresh credentials on every run and stores them in the ignored,
owner-readable `.env` file. Published ports bind only to `127.0.0.1`.

For the engine-backed replay regressions, first build `cargo build --bin inputlayer-server`
from the repository root, then run the adapter tests. Set `INGEST_TEST_ENGINE` to use
another local engine binary. The startup configuration regression requires Docker Compose.
