# Installation

This guide will help you install and run InputLayer on your system.

## Prerequisites

- **Rust 1.75+** - InputLayer is written in Rust
- **Cargo** - Rust's package manager (comes with Rust)

### Installing Rust

If you don't have Rust installed, use [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After installation, restart your terminal or run:
```bash
source $HOME/.cargo/env
```

Verify installation:
```bash
rustc --version  # Should show 1.75.0 or higher
cargo --version
```

## Installation Options

### Option 1: Install from crates.io (Recommended)

```bash
cargo install inputlayer
```

This installs the `inputlayer-client` binary to `~/.cargo/bin/`.

### Option 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/anthropics/inputlayer.git
cd inputlayer

# Build in release mode
cargo build --release

# The binaries are in target/release/
./target/release/inputlayer-client
```

### Option 3: Run without Installing

For quick testing without installing:

```bash
cargo run --release --bin inputlayer-client
```

## Verify Installation

Start the interactive REPL:

```bash
inputlayer-client
```

You should see:

```
InputLayer v0.1.0
Type .help for commands, .quit to exit

inputlayer>
```

Try a simple command:

```
inputlayer> .db list
Databases:
  default
```

Type `.quit` to exit.

## Configuration (Optional)

InputLayer works out of the box with sensible defaults. For customization, create a config file:

**Location options:**
1. `./inputlayer.toml` (current directory)
2. `~/.inputlayer/config.toml` (home directory)

**Example configuration:**

```toml
[storage]
data_dir = "~/.inputlayer/data"
default_database = "default"

[storage.performance]
num_threads = 4

[storage.persist]
buffer_size = 1000
immediate_sync = false
```

## Data Directory

By default, InputLayer stores data in:
- **Linux/macOS:** `~/.inputlayer/data/`
- **Custom:** Set `data_dir` in config file

The data directory contains:
```
~/.inputlayer/data/
├── default/           # Default database
│   ├── relations/     # Base fact storage
│   └── rules/         # Persistent rule definitions
├── persist/           # WAL and batch files
└── metadata/          # System metadata
```

## Next Steps

Now that you have InputLayer installed:

1. **[Your First Program](02-first-program.md)** - Write your first Datalog program
2. **[Core Concepts](03-core-concepts.md)** - Understand facts, rules, and queries
3. **[REPL Guide](04-repl-guide.md)** - Master the interactive environment

## Troubleshooting

### Command not found: inputlayer-client

Make sure `~/.cargo/bin` is in your PATH:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

Add this line to your `~/.bashrc` or `~/.zshrc` for persistence.

### Build fails with missing dependencies

On some Linux systems, you may need development libraries:

```bash
# Ubuntu/Debian
sudo apt-get install build-essential pkg-config libssl-dev

# Fedora
sudo dnf install gcc pkg-config openssl-devel

# macOS (if needed)
xcode-select --install
```

### Permission denied when writing data

Ensure you have write permissions to the data directory:

```bash
mkdir -p ~/.inputlayer/data
chmod 755 ~/.inputlayer/data
```
