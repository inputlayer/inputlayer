# hnsw_rs 0.3.4, patched

This is the [hnsw_rs](https://github.com/jean-pierreBoth/hnswlib-rs) 0.3.4
source from crates.io (upstream commit `673257c6`), used by the engine through
`[patch.crates-io]` in the root `Cargo.toml`. Only `src/`, the licenses and
the README are vendored, with a trimmed `Cargo.toml`. Each patch is marked
`inputlayer patch N` in `src/hnsw.rs`. To see them all, diff against the
registry copy:

```sh
diff -ru ~/.cargo/registry/src/*/hnsw_rs-0.3.4/src vendor/hnsw_rs/src
```

1. **Deadlock in parallel insertion (#377).**
   `reverse_update_neighborhood_simple` held a read lock on the new point's
   neighbour list while it write-locked each neighbour's list. Two inserts
   running at once whose new points list each other each waited for the
   other's write lock forever, so `parallel_insert` (and `.index create`,
   `.index rebuild`, compaction and the rebuild at startup) could hang. The
   patch copies the list and releases its lock first, so an insert never
   holds one point's lock while it waits for another's. Upstream `master`
   (2026-09-26) still holds the lock.
2. **Reverse links at the wrong layer (#302).**
   The same function stored every reverse link at the new point's top layer
   instead of the layer it links on, so layer 0 lacked the reverse links of
   every point above it and could fall apart into pieces a search cannot
   cross: recall@10 on 3000 vectors of dimension 32 fell to 0.59 in about one
   build in 200. Upstream fixed this on `master` ("vidaunited fix on reverse
   edges", unreleased); this applies the same one-line fix. Recall over 200
   builds: before, median 0.984, minimum 0.59; after, median 0.996, minimum
   0.994.
3. **Output on stdout.** Inserting every 50,000th point printed a line to
   stdout; it is logged instead, as on upstream `master`.

Drop this copy once a released hnsw_rs carries patch 1 and patch 2.
