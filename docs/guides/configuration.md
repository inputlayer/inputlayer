# Configuration Guide

InputLayer uses a hierarchical configuration system with multiple sources:

1. `config.toml` - Default configuration file
2. `config.local.toml` - Local overrides (git-ignored)
3. Environment variables (`INPUTLAYER_*` prefix)

## Configuration File Locations

InputLayer looks for config files in this order:
1. `./config.toml` (current directory)
2. `./inputlayer.toml` (alternative name)
3. `~/.inputlayer/config.toml` (home directory)

## Complete Configuration Reference

```toml
# =============================================================================
# STORAGE CONFIGURATION
# =============================================================================
[storage]
# Base directory for all knowledge graph data
data_dir = "./data"

# Default knowledge graph name (created on startup)
default_knowledge_graph = "default"

# Automatically create knowledge graphs when accessed
auto_create_knowledge_graphs = true

# -----------------------------------------------------------------------------
# Legacy Persistence Settings
# -----------------------------------------------------------------------------
[storage.persistence]
# Storage format: parquet, csv, bincode
# - parquet: Columnar format, good compression, recommended for production
# - csv: Human-readable, no compression, good for debugging
# - bincode: Binary format, Rust-specific, fast serialization
format = "parquet"

# Compression: snappy, gzip, none
# - snappy: Fast compression/decompression, good ratio
# - gzip: Better compression ratio, slower
# - none: No compression
compression = "snappy"

# Auto-save interval in seconds (0 = manual save only)
auto_save_interval = 0

# Enable write-ahead logging for crash recovery
enable_wal = true

# -----------------------------------------------------------------------------
# DD-Native Persist Layer (Recommended)
# -----------------------------------------------------------------------------
[storage.persist]
# Enable the DD-native persistence layer
enabled = true

# Buffer size before flushing to disk (number of updates)
buffer_size = 10000

# Durability mode: immediate, batched, async
# - immediate: Sync to disk on each write (safest, slowest)
# - batched: Periodic sync (balanced performance/safety)
# - async: Fire-and-forget (fastest, may lose recent data on crash)
durability_mode = "immediate"

# Compaction window: retain this many historical versions (0 = keep all)
compaction_window = 0

# -----------------------------------------------------------------------------
# Performance Tuning
# -----------------------------------------------------------------------------
[storage.performance]
# Initial capacity for in-memory collections
initial_capacity = 10000

# Batch size for bulk operations
batch_size = 1000

# Enable async I/O operations
async_io = true

# Number of worker threads (0 = use all CPU cores)
num_threads = 0

# =============================================================================
# QUERY OPTIMIZATION
# =============================================================================
[optimization]
# Enable join order planning (experimental, disabled by default)
enable_join_planning = false

# Enable SIP (Sideways Information Passing) rewriting (experimental)
enable_sip_rewriting = false

# Enable subplan sharing across rules
enable_subplan_sharing = true

# Enable boolean specialization optimizations
enable_boolean_specialization = true

# =============================================================================
# LOGGING
# =============================================================================
[logging]
# Log level: trace, debug, info, warn, error
level = "info"

# Log format: text, json
format = "text"

# =============================================================================
# HTTP SERVER (REST API)
# =============================================================================
[http]
# Enable HTTP server for REST API access
enabled = false

# Bind address
host = "127.0.0.1"

# Port number
port = 8080

# CORS allowed origins (empty = allow all in dev mode)
cors_origins = []

# -----------------------------------------------------------------------------
# Web GUI Dashboard
# -----------------------------------------------------------------------------
[http.gui]
# Enable web-based GUI dashboard
enabled = false

# Directory containing GUI static files
static_dir = "./gui/dist"

# -----------------------------------------------------------------------------
# Authentication
# -----------------------------------------------------------------------------
[http.auth]
# Enable JWT-based authentication
enabled = false

# JWT signing secret (CHANGE THIS IN PRODUCTION!)
jwt_secret = "change-me-in-production"

# Session timeout in seconds (default: 24 hours)
session_timeout_secs = 86400
```

## Environment Variables

All config options can be overridden with environment variables using the `INPUTLAYER_` prefix:

```bash
# Storage settings
export INPUTLAYER_STORAGE__DATA_DIR=/var/lib/inputlayer/data
export INPUTLAYER_STORAGE__DEFAULT_KNOWLEDGE_GRAPH=mydb

# Persistence
export INPUTLAYER_STORAGE__PERSIST__DURABILITY_MODE=batched
export INPUTLAYER_STORAGE__PERSIST__BUFFER_SIZE=50000

# HTTP Server
export INPUTLAYER_HTTP__ENABLED=true
export INPUTLAYER_HTTP__PORT=9090

# Logging
export INPUTLAYER_LOGGING__LEVEL=debug
```

**Note:** Use double underscores (`__`) to separate nested config sections.

## Common Configurations

### Development (Fast Iteration)

```toml
[storage]
data_dir = "./dev-data"

[storage.persist]
durability_mode = "async"  # Fast writes, less safe

[logging]
level = "debug"

[http]
enabled = true
```

### Production (Safe & Durable)

```toml
[storage]
data_dir = "/var/lib/inputlayer/data"

[storage.persistence]
format = "parquet"
compression = "snappy"
enable_wal = true

[storage.persist]
durability_mode = "immediate"
buffer_size = 10000

[storage.performance]
num_threads = 0  # Use all cores

[logging]
level = "warn"
format = "json"

[http]
enabled = true
host = "0.0.0.0"
port = 8080

[http.auth]
enabled = true
jwt_secret = "your-secure-secret-here"
```

### High-Throughput Ingestion

```toml
[storage.persist]
durability_mode = "batched"
buffer_size = 100000

[storage.performance]
batch_size = 10000
async_io = true
num_threads = 0
```

### Memory-Constrained Environment

```toml
[storage.performance]
initial_capacity = 1000
batch_size = 100

[storage.persist]
buffer_size = 1000
```

## Durability Modes Explained

| Mode | Write Latency | Crash Safety | Use Case |
|------|--------------|--------------|----------|
| `immediate` | High | Full | Financial data, critical records |
| `batched` | Medium | Partial | Most production workloads |
| `async` | Low | Minimal | Development, analytics pipelines |

### Immediate Mode
- Every write syncs to disk before returning
- Zero data loss on crash
- Highest latency

### Batched Mode
- Writes buffer in memory
- Periodic sync to disk
- May lose last batch on crash

### Async Mode
- Writes return immediately
- Background persistence
- May lose recent updates on crash
- Best for high-throughput ingestion where some loss is acceptable

## Storage Formats

| Format | Size | Speed | Use Case |
|--------|------|-------|----------|
| `parquet` | Smallest | Fast reads | Production, analytics |
| `csv` | Largest | Slow | Debugging, interop |
| `bincode` | Small | Fastest | Rust-only deployments |

## Verifying Configuration

Check your effective configuration:

```bash
# Show loaded config (if available)
./inputlayer --show-config

# Or check in REPL
.status
```
