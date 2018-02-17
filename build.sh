#!/bin/sh

set -e

cargo build --color always
# cargo test --verbose --color always -- --color always --nocapture
