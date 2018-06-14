#!/bin/sh

set -e

cargo build --color always --features gitcache
RUST_BACKTRACE=1 RUST_LOG=dorkfs=debug cargo test --verbose --color always --features gitcache -- --color always --nocapture
