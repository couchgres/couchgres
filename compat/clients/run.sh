#!/usr/bin/env bash
# Run client compatibility checks against a running couchgres.
#
#   COUCHGRES_URL=http://admin:secret@127.0.0.1:5984 compat/clients/run.sh
#
# Needs node/npm and python3 on PATH. Dependencies are installed into
# each client directory (node_modules, .venv) on first run.
set -euo pipefail
cd "$(dirname "$0")"

export COUCHGRES_URL="${COUCHGRES_URL:-http://admin:secret@127.0.0.1:5984}"

echo "== nano =="
(cd nano && npm install --no-audit --no-fund --silent && node check.js)

echo "== pouchdb =="
(cd pouchdb && npm install --no-audit --no-fund --silent && node sync.js)

echo "== couchdb-python =="
(
  cd python
  if [ ! -d .venv ]; then python3 -m venv .venv; fi
  ./.venv/bin/pip install -q -r requirements.txt
  ./.venv/bin/python check.py
)
