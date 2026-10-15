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
from nexus.vault_dbt import run as build_vault
summarise(run())
export(vault=build_vault())   # the Data Vault, in dbt, in the same Spark session
print("wrote assets/data/site.json")
PY
