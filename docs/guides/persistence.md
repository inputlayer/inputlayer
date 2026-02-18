# Persistence Guide

InputLayer provides durable storage with crash recovery, using a combination of Write-Ahead Logging (WAL) and Parquet batch files.

## Architecture Overview

```
Insert/Delete
    ↓
Update{data, time, diff}
    ↓
WAL (immediate durability)
    ↓
In-memory buffer
    ↓ (when buffer full)
Batch file (Parquet)
```

### Recovery Flow

On startup, InputLayer:
1. Loads shard metadata from disk
2. Reads batch files (Parquet)
3. Replays WAL (uncommitted updates)
4. Consolidates to get current state

---

## Directory Structure

```
data/
├── persist/
│   ├── shards/           # Shard metadata (JSON)
│   │   ├── default_edge.json
│   │   └── default_node.json
│   ├── batches/          # Data files (Parquet)
│   │   ├── 1.parquet
│   │   └── 2.parquet
│   └── wal/              # Write-ahead log
│       └── current.wal
```

---

## Durability Modes

InputLayer offers three durability modes, configurable via `config.toml`:

### Immediate Mode (Default)

Every write syncs to disk before returning:

```toml
[storage.persist]
durability_mode = "immediate"
```

| Property | Value |
|----------|-------|
| Write Latency | Highest |
| Crash Safety | Full (zero data loss) |
| Use Case | Financial data, critical records |

### Batched Mode

Writes buffer in memory with periodic sync:

```toml
[storage.persist]
durability_mode = "batched"
buffer_size = 10000
```

| Property | Value |
|----------|-------|
| Write Latency | Medium |
| Crash Safety | Partial (may lose last batch) |
| Use Case | Most production workloads |

### Async Mode

Writes return immediately; background persistence:

```toml
[storage.persist]
durability_mode = "async"
```

| Property | Value |
|----------|-------|
| Write Latency | Lowest |
| Crash Safety | Minimal (may lose recent updates) |
| Use Case | Analytics pipelines, high-throughput ingestion |

---

## Write-Ahead Log (WAL)

The WAL provides O(1) append-only persistence with immediate durability.

### WAL Entry Format

Each entry is a JSON line (for debuggability):

```json
{"op":"insert","relation":"edge","tuples":[[1,2],[3,4]],"ts":1234567890}
{"op":"delete","relation":"edge","tuples":[[1,2]],"ts":1234567891}
```

### WAL Operations

| Field | Description |
|-------|-------------|
| `op` | Operation type: `insert` or `delete` |
| `relation` | Target relation name |
| `tuples` | Array of tuples being modified |
| `ts` | Timestamp (Unix milliseconds) |

### Automatic Compaction

WAL entries are compacted to Parquet when:
- Buffer reaches configured size (`buffer_size`)
- Manual flush is triggered
- Server shutdown (clean)

After compaction, the WAL is archived and cleared.

---

## Batch Files (Parquet)

Data is stored in columnar Parquet format for efficient queries.

### Parquet Schema

Each batch file contains:
- **Data columns**: Your tuple fields
- **time**: Update timestamp (UInt64)
- **diff**: +1 for insert, -1 for delete (Int64)

### Compression

Snappy compression is used by default (fast decompression, good ratio).

### Example File

```
batches/1.parquet
├── col0: Int32 [1, 3, 5]
├── col1: Int32 [2, 4, 6]
├── time: UInt64 [10, 20, 30]
└── diff: Int64 [1, 1, 1]
```

---

## Shards

Each relation is stored as a separate "shard" with its own:
- Metadata file (JSON)
- Batch files (Parquet)
- WAL entries

### Shard Metadata

```json
{
  "name": "default:edge",
  "since": 0,
  "upper": 100,
  "batches": [
    {
      "id": "1",
      "path": "batches/1.parquet",
      "lower": 0,
      "upper": 50,
      "len": 100
    }
  ]
}
```

| Field | Description |
|-------|-------------|
| `name` | Shard identifier (kg:relation) |
| `since` | Lower bound frontier (history discarded before this) |
| `upper` | Upper bound frontier (latest update time + 1) |
| `batches` | List of batch file references |

---

## Compaction

Compaction consolidates history and reclaims space.

### Manual Compaction

```datalog
.compact
```

This:
1. Flushes all pending writes
2. Merges batch files
3. Removes historical entries before `since` frontier
4. Clears WAL

### Automatic Compaction

Configure in `config.toml`:

```toml
[storage.persist]
compaction_window = 1000  # Keep last 1000 versions (0 = keep all)
```

---

## Configuration Reference

```toml
[storage]
# Base directory for all data
data_dir = "./data"

[storage.persist]
# Enable DD-native persistence
enabled = true

# Buffer size before flushing to Parquet
buffer_size = 10000

# Durability: immediate, batched, async
durability_mode = "immediate"

# Compaction window (0 = keep all history)
compaction_window = 0
```

---

## Best Practices

### Development

```toml
[storage.persist]
durability_mode = "async"
buffer_size = 1000
```

Fast iteration, acceptable data loss on crashes.

### Production

```toml
[storage.persist]
durability_mode = "immediate"
buffer_size = 10000
```

Maximum safety, reasonable performance.

### High-Throughput Ingestion

```toml
[storage.persist]
durability_mode = "batched"
buffer_size = 100000

[storage.performance]
batch_size = 10000
async_io = true
```

Balance between throughput and safety.

### Memory-Constrained

```toml
[storage.persist]
buffer_size = 1000
compaction_window = 100

[storage.performance]
initial_capacity = 1000
batch_size = 100
```

Frequent flushes, aggressive compaction.

---

## Monitoring

### Check Storage Status

```bash
.status
```

Shows:
- Data directory location
- Number of shards
- WAL size
- Buffer status

### Check Shard Info

```datalog
.rel
```

Lists all relations with row counts.

---

## Recovery Scenarios

### Normal Startup

1. Load shard metadata
2. Read Parquet batch files
3. Replay WAL entries
4. Consolidate to current state

### Crash Recovery

Same as normal startup. WAL ensures all committed writes are recovered.

### Corrupted Parquet

If a batch file is corrupted:
1. WAL entries for that batch may still be available
2. Manually remove corrupted `.parquet` file
3. Restart to trigger WAL replay

### Corrupted WAL

If WAL is corrupted:
1. Data in Parquet files is safe
2. Uncommitted writes since last flush are lost
3. Rename/remove corrupted WAL file
4. Restart

---

## Differential Updates

InputLayer uses differential dataflow semantics internally:

```rust
Update {
    data: Tuple,    // The actual data
    time: u64,      // Logical timestamp
    diff: i64,      // +1 = insert, -1 = delete
}
```

### Consolidation

Multiple updates to the same tuple are consolidated:

| Updates | Consolidated |
|---------|-------------|
| +1, +1 | +2 (duplicate insert) |
| +1, -1 | 0 (cancelled out) |
| +1, -1, +1 | +1 (net insert) |

This enables:
- Efficient delta storage
- Time-travel queries (if history preserved)
- Incremental computation

---

## Troubleshooting

### High Write Latency

**Cause**: Immediate durability mode with slow disk

**Solutions**:
1. Switch to `batched` durability mode
2. Use faster storage (SSD)
3. Increase `buffer_size` to batch more writes

### High Memory Usage

**Cause**: Large buffer, many shards

**Solutions**:
1. Reduce `buffer_size`
2. Enable compaction (`compaction_window > 0`)
3. Flush more frequently

### Slow Startup

**Cause**: Large WAL, many batch files

**Solutions**:
1. Run `.compact` before shutdown
2. Enable compaction window
3. Flush buffers before shutdown

### Missing Data After Crash

**Cause**: Async durability mode

**Solutions**:
1. Switch to `immediate` or `batched` mode
2. Accept trade-off for async mode

---

## Next Steps

- [Configuration Guide](configuration.md) - Full configuration reference
- [WebSocket API (AsyncAPI)](../spec/asyncapi.yaml) - Programmatic access
- [Temporal Functions](temporal.md) - Time-based queries on persisted data
