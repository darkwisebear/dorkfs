#!/bin/sh

set -e

cargo build --color always --features fuse
RUST_BACKTRACE=1 RUST_LOG=dorkfs=debug cargo test --verbose --color always -- --color always --nocapture
