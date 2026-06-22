#!/usr/bin/env bash
# Portable one-shot setup: downloads the matching tursodb CLI and builds the Rust harness.
# Works on macOS (arm64/x86_64) and Linux/WSL (x86_64/aarch64). No machine-specific paths.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${TURSO_EXP_HOME:-$REPO_ROOT/.work}/bin"
mkdir -p "$BIN_DIR"

TURSO_VERSION="${TURSO_VERSION:-v0.6.1}"   # keep in lockstep with the `turso` crate pin in Cargo.toml

# ---- 1. detect platform -> release asset triple ----
OS="$(uname -s)"; ARCH="$(uname -m)"
case "$OS" in
  Darwin) PLAT="apple-darwin" ;;
  Linux)  PLAT="unknown-linux-gnu" ;;
  *) echo "Unsupported OS: $OS (need macOS or Linux). On Windows use WSL2." >&2; exit 1 ;;
esac
case "$ARCH" in
  arm64|aarch64) CPU="aarch64" ;;
  x86_64|amd64)  CPU="x86_64" ;;
  *) echo "Unsupported arch: $ARCH" >&2; exit 1 ;;
esac
ASSET="turso_cli-${CPU}-${PLAT}.tar.xz"
URL="https://github.com/tursodatabase/turso/releases/download/${TURSO_VERSION}/${ASSET}"

# ---- 2. download + extract the tursodb CLI (used as the --sync-server hub) ----
if [ -z "$(find "$BIN_DIR" -name tursodb -type f 2>/dev/null | head -1)" ]; then
  echo "Downloading $ASSET ($TURSO_VERSION)..."
  curl -sSL "$URL" -o "$BIN_DIR/cli.tar.xz"
  tar xf "$BIN_DIR/cli.tar.xz" -C "$BIN_DIR"
  rm -f "$BIN_DIR/cli.tar.xz"
fi
TURSODB="$(find "$BIN_DIR" -name tursodb -type f | head -1)"
chmod +x "$TURSODB"
echo "tursodb: $TURSODB ($("$TURSODB" --version 2>&1 | head -1))"
"$TURSODB" --help 2>&1 | grep -q -- "--sync-server" || { echo "WARN: this CLI lacks --sync-server"; }

# ---- 3. Rust toolchain check ----
if ! command -v cargo >/dev/null 2>&1; then
  echo "ERROR: cargo not found. Install Rust (>= 1.92): https://rustup.rs" >&2
  exit 1
fi
echo "cargo: $(cargo --version)"

# ---- 4. build the harness (release) ----
echo "Building cascade (cargo build --release)..."
( cd "$REPO_ROOT" && cargo build --release )
echo "built: $REPO_ROOT/target/release/cascade"

echo
echo "Setup complete. Next:"
echo "  ./run.sh                          # synthetic data, full pipeline + benchmarks"
echo "  PATENTS_JSONL=/path/to.jsonl ./run.sh   # use real data instead"
