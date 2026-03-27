# Retail Demo: Printer Ink Recommendations

This example demonstrates the printer ink scenario from the project README -
compatible product recommendations from purchase history and live inventory,
using rules instead of glue code.

## What it shows

1. **Two rules** replace four separate API calls (purchase history, compatibility
   API, inventory filter, re-ranking)
2. **Vector search + rules in one query** - similarity ranking combined with
   compatibility logic
3. **Live retraction** - when stock runs out, recommendations update instantly
4. **Incremental derivation** - when the shopper buys a new printer, compatible
   products appear automatically

## Running it

Start the server in one terminal:

```bash
cargo run --release --bin inputlayer-server
```

Run the demo in another:

```bash
cargo run --bin inputlayer-client -- --script examples/retail/printer-ink.idl
```

Or run the demo and drop into the REPL to explore interactively:

```bash
cargo run --bin inputlayer-client -- --script examples/retail/printer-ink.idl --repl
```

## What to look for

- **Query 3** is the key one: `epson-202-black` and `brother-lc3013` have nearly
  identical similarity scores to the Canon cartridges but do not appear - the
  compatibility rules excluded them
- After the stock retraction, `canon-pg-245xl` disappears from results
- After the new printer purchase, Epson 522 products appear without any query change
