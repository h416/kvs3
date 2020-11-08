#!/bin/bash

if [ $# -eq 0 ]; then
  echo "usage ./task.sh |check|update|test|build|" 1>&2
  exit 1
fi


if [ $1 = "check" ] ; then
  cargo fmt && cargo fix --allow-no-vcs && cargo clippy && cargo check
elif [ $1 = "update" ]; then
  cargo update
elif [ $1 = "test" ]; then
  cargo test
elif [ $1 = "build" ]; then
  cargo build --release
else
  echo "unknown command"
fi