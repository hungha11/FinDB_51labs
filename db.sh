#!/usr/bin/env bash
# Usage: ./db.sh start|stop|status
cd "$(dirname "$0")"
export JAVA_HOME="$PWD/.runtime/jdk/Contents/Home"
exec .runtime/questdb/questdb.sh "${1:-status}" -d "$PWD/data"
