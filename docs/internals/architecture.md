# InputLayer Architecture

This file used to hold a hand-maintained copy of the architecture reference. That copy described a rule-materialization subsystem that does not run on the server, so it has been retired.

The architecture reference lives in [`docs/content/docs/internals/architecture.mdx`](../content/docs/internals/architecture.mdx), the single source of truth for the documentation (see [`docs/README.md`](../README.md)).

In short, today: persistent rules are stored definitions evaluated from the base facts on every query that reads them, each query runs as a fresh Differential Dataflow dataflow over a snapshot, and subscriptions re-evaluate their query after each relevant commit and send the diff. Making deployed rules incrementally maintained views is milestone 9 ([#305](https://github.com/inputlayer/inputlayer/issues/305)).
