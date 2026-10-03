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
| `docker-compose.yml` | the stack; demo secrets to replace before exposing any port |
| `debezium/application.properties` | Debezium Server: Postgres `pgoutput` to the adapter's `/cdc` |
| `postgres/init.sql` | the demo source table |
| `adapter/ingest/mappings.py` | which tables and webhook events feed which relations: edit this |
| `adapter/ingest/` | the adapter (`server.py`), the watching client (`watch.py`), the demo (`demo.py`) |
| `adapter/tests/` | unit tests: `cd adapter && pip install -e '.[dev]' && pytest` |
