#!/bin/sh

set -e

cargo build --color always --features fuse
# cargo test --verbose --color always -- --color always --nocapture
