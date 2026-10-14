# NEXUS for Quilter

An independent Lead Data Engineer portfolio by R. Firman Insan Muhammad (VIM), for the Chief Data Office at Quilter.

**Live site:** https://rfim.github.io/nexus-quilter/ · **Lakehouse:** https://rfim.github.io/nexus-quilter/lakehouse.html · **Simulations:** https://rfim.github.io/nexus-quilter/simulate.html · **Fireside deck:** [assets/Nexus_Quilter_VIM.pdf](assets/Nexus_Quilter_VIM.pdf)

The site shows how I'd serve Quilter's Finance, Marketing, HR, Executive and Product teams. Every agreement with a department ends up as a YAML data contract, and the contract compiles into checks that run on every load. The numbers on the site come from a real run of the Databricks Python in this repository, on synthetic wealth-platform data.

## What's here

| Path | What it does |
|---|---|
| `src/nexus/generate.py` | Synthetic feeds: clients, accounts, funds, platform transactions, ledger, service journeys, colleagues, marketing spend. Includes replayed records, malformed rows and one planted ledger break, so the controls have real work to do. |
| `src/nexus/bronze.py` | Lands each feed as Delta through an ordered MERGE. Replaying a feed changes nothing. |
| `src/nexus/silver.py` | Splits transactions by the source contract's rules (quarantine with the failed rule attached), builds the SCD2 fund dimension and conformed tables. |
| `src/nexus/gold.py` | Incremental fact with a 72-hour lookback and point-in-time fund join, then one data product per department. |
| `src/nexus/contracts.py` | Compiles each department contract into SQL checks, a Lakeflow expectation set and Unity Catalog grants, and runs the checks. |
| `contracts/` | Source contracts (`sources/`) and the five department contracts (`products/`). |
| `databricks.yml` | Asset Bundle for running the same package as a Databricks job. |
| `index.html`, `assets/` | The static site, published with GitHub Pages. |
| `simulate.html`, `assets/sim/` | The ELT and stakeholder simulations, starting from the raw landing files. |
| `lakehouse.html`, `assets/lakehouse/` | The lakehouse in the browser: the run's real Delta tables (Parquet + `_delta_log`), replayed client-side. |

## Simulations

`simulate.html` maps the job description to two hands-on simulations:

- **ELT end to end** (engineering delivery, operational excellence). The browser fetches the raw landing files and runs the pipeline as DuckDB SQL: MERGE into bronze, quality split from the source contract, SCD2 and conformed silver, the incremental fact and five products, every department's contract checks, a late batch and a restatement, then messages to the data owners. Scenarios: replay every feed, malformed rows, late corrections, an upstream schema change (the load stops before writing, with an incident card), and the planted ledger break. With the defaults it matches the Spark run: 13,022 in bronze, 15 quarantined, 13,007 valid, 39/40 checks.
- **Stakeholder week** (cross-functional collaboration, influence). Ten requests from Finance, Marketing, HR, Executive and Product; each choice moves department trust, team focus and platform risk, with my reasoning after each one.

## The lakehouse in your browser

`lakehouse.html` loads the Delta tables this run wrote, exactly as Spark left them. The browser:

- **replays each `_delta_log`** (add/remove actions) to find the files at any version, so `DESCRIBE HISTORY`, `DESCRIBE DETAIL`, `VERSION AS OF n` and `TIMESTAMP AS OF '…'` come from the real commits;
- **decodes the Parquet** with [parquet-wasm](https://github.com/kylebarron/parquet-wasm) and runs SQL in [DuckDB-WASM](https://github.com/duckdb/duckdb-wasm). No DuckDB extension is downloaded, so it works on networks that block `extensions.duckdb.org`;
- **simulates Unity Catalog**: three-level names (`nexus.gold.…`), grants from the department contracts, column masks on personal data, and a row filter on legal entity, depending on the persona you "run as".

Five notebooks walk through it: explore, MERGE and safe replay, time travel (as issued vs now), Unity Catalog permissions, and the contract checks. A Workflows tab replays the recorded task timings. It is a simulation of Databricks behaviour, not Databricks itself.

## The last run

- 13,202 landed transaction records → 13,022 in bronze; replaying every feed: 13,022 (no change)
- A late batch of 40 corrections a day later: MERGE updates them in place, and Finance's product is restated as Delta version 1 (version 0 is "as issued")
- 15 malformed rows quarantined (9 bad ISINs, 6 unknown transaction types)
- Fact table 13,007 rows, unchanged after an incremental re-run
- Contracts: 39 of 40 checks pass. The failure is the planted £250 ledger break on 14 Aug 2026, caught by Finance's reconciliation rule.

## Run it

Python 3.12+ and Java 17+:

```bash
pip install -r requirements.txt
python -m pytest -q          # unit tests
./scripts/build.sh           # full run; rewrites assets/data/site.json
python -m http.server 8000   # then open http://localhost:8000/
```

Locally, tables live in a Spark catalog named `nexus_<layer>`. On Databricks, set `NEXUS_CATALOG=nexus` and the same code writes to `nexus.<layer>.<table>` under Unity Catalog.

## What's real

- **Real:** the Lifepal outcomes on the site (from my CV and production work) and all the code here, which runs end to end in CI.
- **Synthetic:** every client, colleague, fund and transaction. No Quilter or Lifepal data is used.
- **Depth:** in production I've run Databricks within Azure (Delta on ADLS Gen2, Workflows, ADF, Key Vault, Spark tuning, Kafka and Debezium CDC). I know Unity Catalog, Asset Bundles and Lakeflow declarative pipelines at working level, and Data Vault and Fabric at concept level.

The site uses Segoe UI where it's installed, and otherwise [Selawik](https://github.com/microsoft/Selawik), Microsoft's open-source metric-compatible font (SIL OFL 1.1, licence in `assets/fonts/`).

This is an independent application. It is not affiliated with or endorsed by Quilter plc.
