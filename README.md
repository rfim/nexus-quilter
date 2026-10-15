# NEXUS for Quilter

An independent Lead Data Engineer portfolio by R. Firman Insan Muhammad (VIM), for the Chief Data Office at Quilter.

**Live site:** https://rfim.github.io/nexus-quilter/ · **Lakehouse:** https://rfim.github.io/nexus-quilter/lakehouse.html · **How I work:** https://rfim.github.io/nexus-quilter/simulate.html · **Fireside deck:** [assets/Nexus_Quilter_VIM.pdf](assets/Nexus_Quilter_VIM.pdf) · **One page:** [assets/VIM_Quilter_one_page.pdf](assets/VIM_Quilter_one_page.pdf) · **Companion note:** [assets/VIM_Quilter_companion_note.pdf](assets/VIM_Quilter_companion_note.pdf)

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
| `simulate.html`, `assets/sim/` | How I work: the start of the story, “How the data gets there” (the ELT run) and chapter 4 (a week with the business). |
| `design.html`, `release.html`, `review.html`, `oncall.html`, `duty.html`, `streaming.html`, `ask.html`, `vault.html` | Chapters 1–3 and 5–7 of How I work, one per part of the job description, all on the same lakehouse (`assets/engine.js`, `assets/jdkit.js`). |
| `lakehouse.html`, `assets/lakehouse/` | The lakehouse in the browser: the run's real Delta tables (Parquet + `_delta_log`), replayed client-side. |

## How I work: lead and serve

`simulate.html` starts the story: chapters about leading (design it, ship it, grow the team) and four about serving (the business, on-call, Consumer Duty, real time). Every chapter runs on the same in-browser lakehouse (`assets/engine.js`), so the numbers agree from page to page.

- **ELT end to end** (engineering delivery, operational excellence). The browser fetches the raw landing files and runs the pipeline as DuckDB SQL: MERGE into bronze, quality split from the source contract, SCD2 and conformed silver, the incremental fact and five products, every department's contract checks, a late batch and a restatement, then messages to the data owners. Scenarios: replay every feed, malformed rows, late corrections, an upstream schema change (the load stops before writing, with an incident card), and the planted ledger break. With the defaults it matches the Spark run: 13,022 in bronze, 15 quarantined, 13,007 valid, 39/40 checks.
- **Stakeholder week** (4 · cross-functional collaboration). Thirteen requests from Finance, Marketing, HR, Executive, Product and the architecture, platform and governance teams; each choice moves trust, team focus and platform risk, with my reasoning after each one.
- **Design review** (1 · technical leadership). Six design decisions for a Consumer Duty price-and-value product, each with an evidence query on the lakehouse, ending in a downloadable ADR.
- **Requirement to release** (2 · engineering delivery). Clarifying questions, a contract diff with its blast radius, a plan, and a CI gate whose metric diff is computed on the Delta tables (the buggy version is blocked at 1.07%).
- **Review a PR** (3 · mentoring). A new engineer's PySpark PR with a committed key, PII in gold, append instead of MERGE, `toPandas()` and no test; flag the lines, then choose comments that teach. The append bug is proved on bronze.
- **On-call shift** (5 · operational excellence). Live SLO tiles, triage, the planted £250 reconciliation break investigated with three queries, an incident record with time to acknowledge and mitigate, and a volume alert tuned against a year of real daily counts.
- **Consumer Duty** (regulatory). The analyst can't read the vulnerability flag; the pipeline's service principal publishes an aggregate with a minimum cell size, which shows vulnerable clients waiting about twice as long on transfers out.
- **Streaming** (batch and real-time). 13,242 events in micro-batches, each a real MERGE, with a watermarked keyed aggregate. With MERGE and no watermark it matches the lakehouse exactly; the other settings show why the certified numbers stay on batch.
- **Bring it together** (data modelling). A Data Vault over three Quilter businesses that describe the same client (FNZ platform, adviser CRM, Quilter Cheviot): hub on the standardised NI number, links, insert-only satellites with hashdiffs, a next-day change loaded without updates, an audit question answered from history, and a point-in-time Kimball dimension on top. Also a tab in the Live lakehouse.
- **Let them ask** (self-service and AI). Part 1 builds the semantic layer: three reports give three numbers for net flows, Finance's definition is agreed, written as a metric view, tested live (equals the certified product, grain, ledger) and published with grants and chatbot synonyms. Part 2 is the semantic layer as metric views, and a chat that maps a question to a certified measure, compiles it to SQL, runs it on the lakehouse with the asker's permissions and cites the source. It refuses undefined metrics, personal data and advice. A small rule-based parser stands in for the LLM.

## The lakehouse in your browser

`lakehouse.html` loads the Delta tables this run wrote, exactly as Spark left them. The browser:

- **replays each `_delta_log`** (add/remove actions) to find the files at any version, so `DESCRIBE HISTORY`, `DESCRIBE DETAIL`, `VERSION AS OF n` and `TIMESTAMP AS OF '…'` come from the real commits;
- **decodes the Parquet** with [parquet-wasm](https://github.com/kylebarron/parquet-wasm) and runs SQL in [DuckDB-WASM](https://github.com/duckdb/duckdb-wasm). No DuckDB extension is downloaded, so it works on networks that block `extensions.duckdb.org`;
- **simulates Unity Catalog**: three-level names (`nexus.gold.…`), grants from the department contracts, column masks on personal data, and a row filter on legal entity, depending on the persona you "run as".

Four platform views open it: **Architecture** (the end-to-end flow, animated, with the run's real counts), **Orchestration** (the recorded job run replayed, including a failure and a repair run), **Lineage & governance** (every table's upstream and blast radius, with PII tags and grants for the chosen persona) and **Cost** (the levers and a small estimator), plus **Data Vault** (chapter 9, run in place). Then five notebooks walk through it: explore, MERGE and safe replay, time travel (as issued vs now), Unity Catalog permissions, and the contract checks. A Workflows tab replays the recorded task timings. It reproduces Databricks behaviour in the browser; it is not Databricks itself.

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
