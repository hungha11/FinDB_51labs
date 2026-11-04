#!/usr/bin/env bash
# Run every table ingest in tables/. Used by cron.
cd "$(dirname "$0")"
shopt -s nullglob
PY=/opt/miniconda3/envs/findb/bin/python
for f in tables/*.py; do
  name=$(basename "$f" .py)
  echo "== $name"
  $PY -m "tables.$name" || echo "!! $name failed"
done
