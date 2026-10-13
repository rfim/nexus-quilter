#!/usr/bin/env bash
# Rebuild everything the site shows from a clean run: data, pipeline, contracts, export.
# Locally the catalog lives for one Spark session, so the run and the export share a process.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf target landing
export PYTHONPATH=src
python - <<'PY'
from nexus.pipeline import run, summarise
from nexus.export import export
summarise(run())
export()
print("wrote assets/data/site.json")
PY
