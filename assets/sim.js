// NEXUS Simulations.
// 1. ELT end to end: the pipeline in src/nexus/, rewritten as DuckDB SQL and run in the browser from
//    the raw landing files. Same rules (from the source contract), same products, same contract checks.
// 2. Stakeholder week: a scenario game scored on department trust, team focus and platform risk.
const DUCKDB = "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";
const $ = (s, r = document) => r.querySelector(s);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
const fmt = (n) => Number(n).toLocaleString("en-GB");

// ---- tabs -----------------------------------------------------------------------------------
function showTab(key) {
  document.querySelectorAll(".sim-tabs button").forEach((b) => b.setAttribute("aria-selected", b.dataset.tab === key));
  $("#tab-elt").hidden = key !== "elt";
  $("#tab-people").hidden = key !== "people";
}
document.querySelectorAll("[data-tab]").forEach((b) => b.addEventListener("click", () => {
  showTab(b.dataset.tab);
  if (b.classList.contains("jd-card")) $(".sim-tabs").scrollIntoView({ behavior: "smooth" });
}));

// =============================================================================================
// 1. ELT END TO END
// =============================================================================================
let duck, db, conn, site;

async function boot() {
  duck = await import(DUCKDB);
  const bundle = await duck.selectBundle(duck.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  db = new duck.AsyncDuckDB(new duck.VoidLogger(), new Worker(workerUrl));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  await db.open({ query: { castBigIntToDouble: true, castDecimalToDouble: true } });
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();
  site = await (await fetch("assets/data/site.json")).json();
  const v = (await conn.query("SELECT version() AS v")).toArray()[0].v;
  return v;
}

const SCENARIOS = [
  { key: "replay", label: "Replay every feed", hint: "The same files arrive twice. MERGE should change nothing.", on: true },
  { key: "malformed", label: "Malformed rows in the feed", hint: "15 rows with a bad ISIN or an unknown transaction type.", on: true },
  { key: "late", label: "Late corrections batch", hint: "40 corrections arrive after the first publish. Finance's product is restated.", on: true },
  { key: "drift", label: "Upstream schema change", hint: "The platform team renames amount_gbp to amount in the late batch.", on: false },
  { key: "ledger", label: "Ledger posting the platform never got", hint: "A £250 ISA posting on 14 Aug exists only in the ledger.", on: true },
];

const TASKS = [
  { key: "extract", label: "Extract", small: "landing files" },
  { key: "bronze", label: "Load bronze", small: "MERGE" },
  { key: "replay", label: "Replay", small: "MERGE again" },
  { key: "quality", label: "Quality split", small: "contract rules" },
  { key: "silver", label: "Silver", small: "SCD2 · conformed" },
  { key: "gold", label: "Gold", small: "fact + 5 products" },
  { key: "check1", label: "Contracts", small: "first publish" },
  { key: "late", label: "Late batch", small: "schema check · MERGE" },
  { key: "restate", label: "Restate", small: "silver + gold" },
  { key: "check2", label: "Contracts", small: "after restatement" },
  { key: "notify", label: "Notify owners", small: "from the contracts" },
];
const state = {};   // task key -> {status, sql:[], metrics:{}, sample, startedAt, ms}

function log(text, cls = "") {
  const c = $("#console");
  if (c.querySelector(".muted") && !c.dataset.started) { c.innerHTML = ""; c.dataset.started = "1"; }
  const t = new Date().toISOString().slice(11, 19);
  c.insertAdjacentHTML("beforeend", `<div class="${cls}">[${t}] ${esc(text)}</div>`);
  c.scrollTop = c.scrollHeight;
}

function renderDag() {
  $("#dag").innerHTML = TASKS.map((t, i) => {
    const s = state[t.key] || {};
    const status = s.status || "pending";
    const line = status === "pending" ? "waiting" : status === "running" ? "running…" : status === "skipped" ? "skipped" : s.summary || status;
    return `<li><button class="${status}" data-key="${t.key}" aria-pressed="false"><small>${i + 1} · ${esc(t.small)}</small><b>${esc(t.label)}</b><span>${esc(line)}</span></button></li>`;
  }).join("");
  document.querySelectorAll("#dag button").forEach((b) => b.onclick = () => showDetail(b.dataset.key));
}

function showDetail(key) {
  document.querySelectorAll("#dag button").forEach((b) => b.setAttribute("aria-pressed", b.dataset.key === key));
  const t = TASKS.find((x) => x.key === key), s = state[key];
  if (!s || s.status === "pending") { $("#detail").innerHTML = `<h3>${esc(t.label)}</h3><p class="muted">Not run yet.</p>`; return; }
  const metrics = Object.entries(s.metrics || {}).map(([k, v]) => `<span>${esc(k)}: ${esc(typeof v === "number" ? fmt(v) : v)}</span>`).join("");
  const sample = s.sample && s.sample.rows.length ? `<h4>Sample output</h4><div class="tbl"><table><tr>${s.sample.cols.map((c) => `<th>${esc(c)}</th>`).join("")}</tr>${
    s.sample.rows.map((r) => `<tr>${r.map((v) => `<td>${v == null ? '<span class="muted">null</span>' : esc(v)}</td>`).join("")}</tr>`).join("")}</table></div>` : "";
  $("#detail").innerHTML = `<h3>${esc(t.label)} <span class="muted">· ${esc(s.status)}${s.ms != null ? ` · ${s.ms} ms` : ""}</span></h3>
    ${s.error ? `<p class="fail">${esc(s.error)}</p>` : ""}<div class="metrics">${metrics}</div>
    ${s.sql.length ? `<h4>SQL it ran</h4><pre>${esc(s.sql.join(";\n\n") + ";")}</pre>` : ""}${sample}`;
}

async function q(task, sql) {
  state[task].sql.push(sql.trim());
  return conn.query(sql);
}
async function scalar(sql) { return Object.values((await conn.query(sql)).toArray()[0].toJSON())[0]; }
async function sampleOf(sql, n = 8) {
  const t = await conn.query(`${sql} LIMIT ${n}`);
  return { cols: t.schema.fields.map((f) => f.name), rows: t.toArray().map((r) => Object.values(r.toJSON()).map((v) =>
    v == null ? null : (typeof v === "number" && Math.abs(v) > 1e11 ? new Date(v > 1e14 ? v / 1000 : v).toISOString().slice(0, 19) : (typeof v === "object" ? JSON.stringify(v) : v)))) };
}

async function step(key, fn) {
  state[key] = { status: "running", sql: [], metrics: {} };
  renderDag();
  const t0 = performance.now();
  try {
    const out = await fn(state[key]);
    state[key].status = state[key].status === "failed" ? "failed" : "success";
    state[key].ms = Math.round(performance.now() - t0);
    renderDag();
    return out;
  } catch (e) {
    Object.assign(state[key], { status: "failed", error: e.message, ms: Math.round(performance.now() - t0) });
    renderDag();
    throw e;
  }
}
function skip(key, why) { state[key] = { status: "skipped", sql: [], metrics: { reason: why } }; renderDag(); }

// ---- Extract: fetch the landing files and load them as raw tables ---------------------------
const lit = (v) => v == null ? "NULL" : typeof v === "number" ? String(v) : typeof v === "boolean" ? (v ? "TRUE" : "FALSE") : `'${String(v).replace(/'/g, "''")}'`;
function sqlType(rows, col) {
  const v = rows.find((r) => r[col] != null)?.[col];
  if (typeof v === "boolean") return "BOOLEAN";
  if (typeof v === "number") return rows.every((r) => r[col] == null || Number.isInteger(r[col])) ? "BIGINT" : "DOUBLE";
  return "VARCHAR";
}
async function loadRaw(task, name, rows) {
  const cols = Object.keys(rows[0]);
  await q(task, `CREATE OR REPLACE TABLE raw.${name} (${cols.map((c) => `"${c}" ${sqlType(rows, c)}`).join(", ")})`);
  for (let i = 0; i < rows.length; i += 1500) {
    const values = rows.slice(i, i + 1500).map((r) => `(${cols.map((c) => lit(r[c])).join(",")})`).join(",");
    await conn.query(`INSERT INTO raw.${name} VALUES ${values}`);
  }
  state[task].sql.push(`INSERT INTO raw.${name} VALUES (…)  -- ${fmt(rows.length)} rows from landing/${name}.jsonl`);
  return cols;
}
async function fetchFeed(name) {
  const text = await (await fetch(`assets/sim/landing/${name}.jsonl`)).text();
  return text.trim().split("\n").map((l) => JSON.parse(l));
}

// ---- Load: MERGE into bronze (DuckDB's INSERT … ON CONFLICT is the MERGE) -------------------
async function mergeBronze(task, source, rawName) {
  const tgt = source.target, keys = tgt.merge_keys, order = tgt.ordering_key;
  const table = `sim.bronze.${tgt.table}`;
  const cols = (await conn.query(`DESCRIBE raw.${rawName}`)).toArray().map((r) => r.toJSON());
  const exists = await scalar(`SELECT count(*) FROM duckdb_tables() WHERE database_name='sim' AND schema_name='bronze' AND table_name='${tgt.table}'`);
  if (!exists) {
    await q(task, `CREATE TABLE ${table} (${cols.map((c) => `"${c.column_name}" ${c.column_type}`).join(", ")}, _ingested_at TIMESTAMP, PRIMARY KEY (${keys.join(", ")}))`);
  } else {
    // schema check: the contract's interface must not change silently
    const have = new Set((await conn.query(`DESCRIBE ${table}`)).toArray().map((r) => r.toJSON().column_name));
    const incoming = new Set(cols.map((c) => c.column_name));
    const missing = [...have].filter((c) => c !== "_ingested_at" && !incoming.has(c));
    const added = [...incoming].filter((c) => !have.has(c));
    if (missing.length || added.length) {
      throw new Error(`Schema drift on ${source.feed}: missing [${missing.join(", ")}], unexpected [${added.join(", ")}]. Stream stopped before writing; last good version kept.`);
    }
  }
  const before = await scalar(`SELECT count(*) FROM ${table}`);
  const dedup = `SELECT *, now() AS _ingested_at FROM raw.${rawName} QUALIFY row_number() OVER (PARTITION BY ${keys.join(", ")}${order ? ` ORDER BY ${order} DESC` : ""}) = 1`;
  const updates = order ? await scalar(`SELECT count(*) FROM (${dedup}) s JOIN ${table} t USING (${keys.join(", ")}) WHERE s.${order} > t.${order}`) : 0;
  const setCols = cols.map((c) => c.column_name).filter((c) => !keys.includes(c));
  await q(task, `INSERT INTO ${table}
${dedup}
ON CONFLICT (${keys.join(", ")}) ${order
    ? `DO UPDATE SET ${[...setCols, "_ingested_at"].map((c) => `"${c}" = excluded."${c}"`).join(", ")}\nWHERE excluded.${order} > ${tgt.table}.${order}`
    : "DO NOTHING"}`);
  const after = await scalar(`SELECT count(*) FROM ${table}`);
  return { inserted: after - before, updated: updates, rows: after };
}

// ---- Transform: the PySpark logic, as DuckDB SQL -------------------------------------------
function rulesToDuck(rules) {
  return Object.fromEntries(Object.entries(rules).map(([k, v]) => [k, v.replace(/(\w+)\s+RLIKE\s+'([^']*)'/g, "regexp_matches($1, '$2')")]));
}
const MONTHS = "SELECT unnest(generate_series(DATE '2025-10-01', DATE '2026-09-01', INTERVAL 1 MONTH))::DATE AS month";

async function buildSilver(task) {
  const tx = site.sources.find((s) => s.feed === "platform_transactions");
  const rules = rulesToDuck(tx.quality.rules);
  const passed = Object.values(rules).map((r) => `(${r})`).join(" AND ");
  await q(task, `CREATE OR REPLACE TEMP VIEW typed_tx AS SELECT * REPLACE (CAST(trade_date AS DATE) AS trade_date) FROM sim.bronze.platform_transaction`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.platform_transaction AS SELECT * FROM typed_tx WHERE ${passed} AND op <> 'D'`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.platform_transaction_quarantine AS
SELECT *, list_filter([${Object.entries(rules).map(([n, r]) => `CASE WHEN NOT (${r}) THEN '${n}' END`).join(", ")}], x -> x IS NOT NULL) AS _failed_rules,
       now() AS _quarantined_at
FROM typed_tx WHERE NOT (${passed})`);
  const valid = await scalar("SELECT count(*) FROM sim.silver.platform_transaction");
  const quarantined = await scalar("SELECT count(*) FROM sim.silver.platform_transaction_quarantine");
  const reasons = (await conn.query("SELECT rule, count(*) AS n FROM (SELECT unnest(_failed_rules) AS rule FROM sim.silver.platform_transaction_quarantine) GROUP BY 1 ORDER BY 1")).toArray()
    .map((r) => `${r.toJSON().rule} ${r.toJSON().n}`).join(", ");
  return { valid, quarantined, reasons };
}

async function buildConformed(task) {
  await q(task, `CREATE OR REPLACE TABLE sim.silver.dim_fund_share_class AS
SELECT hash(c.isin, c.effective_from) AS fund_sk, c.isin AS fund_nk, f.fund_name, f.asset_class,
       c.ongoing_charge_pct, c.risk_rating, CAST(c.effective_from AS DATE) AS valid_from,
       coalesce(lead(CAST(c.effective_from AS DATE)) OVER (PARTITION BY c.isin ORDER BY c.effective_from), DATE '9999-12-31') AS valid_to
FROM sim.bronze.fund_charge c JOIN sim.bronze.fund f USING (isin)`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.dim_client AS SELECT * REPLACE (CAST(joined_on AS DATE) AS joined_on), hash(client_id) AS client_sk FROM sim.bronze.client`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.account AS SELECT * REPLACE (CAST(opened_on AS DATE) AS opened_on) FROM sim.bronze.account`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.service_journey AS SELECT * REPLACE (CAST(created_on AS DATE) AS created_on, CAST(completed_on AS DATE) AS completed_on) FROM sim.bronze.service_journey`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.colleague AS SELECT * REPLACE (CAST(hired_on AS DATE) AS hired_on, CAST(left_on AS DATE) AS left_on) FROM sim.bronze.colleague`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.finance_ledger AS SELECT * REPLACE (CAST(trade_date AS DATE) AS trade_date) FROM sim.bronze.finance_ledger`);
  await q(task, `CREATE OR REPLACE TABLE sim.silver.marketing_spend AS SELECT * REPLACE (CAST(month AS DATE) AS month) FROM sim.bronze.marketing_spend`);
  return { scd2_versions: await scalar("SELECT count(*) FROM sim.silver.dim_fund_share_class"), repriced_funds: await scalar("SELECT count(*) - count(DISTINCT fund_nk) FROM sim.silver.dim_fund_share_class") };
}

const FACT_COLS = ["transaction_sk", "transaction_id", "fund_sk", "ongoing_charge_pct", "account_id", "client_sk", "wrapper_code",
  "legal_entity", "channel", "trade_date", "transaction_type", "amount_gbp", "_ingested_at"];
async function buildGold(task) {
  const exists = await scalar("SELECT count(*) FROM duckdb_tables() WHERE database_name='sim' AND schema_name='gold' AND table_name='fct_platform_transaction'");
  if (!exists) {
    await q(task, `CREATE TABLE sim.gold.fct_platform_transaction (transaction_sk UBIGINT PRIMARY KEY, transaction_id VARCHAR, fund_sk UBIGINT,
  ongoing_charge_pct DOUBLE, account_id VARCHAR, client_sk UBIGINT, wrapper_code VARCHAR, legal_entity VARCHAR, channel VARCHAR,
  trade_date DATE, transaction_type VARCHAR, amount_gbp DOUBLE, _ingested_at TIMESTAMP)`);
  }
  const before = await scalar("SELECT count(*) FROM sim.gold.fct_platform_transaction");
  // incremental: only rows ingested inside the 72-hour lookback (p99.5 of late arrivals), merged on the key
  await q(task, `INSERT INTO sim.gold.fct_platform_transaction
SELECT hash(s.transaction_id), s.transaction_id, f.fund_sk, f.ongoing_charge_pct, s.account_id, c.client_sk, s.wrapper_code,
       c.legal_entity, c.channel, s.trade_date, s.transaction_type, s.amount_gbp, s._ingested_at
FROM sim.silver.platform_transaction s
LEFT JOIN sim.silver.dim_fund_share_class f            -- point in time: the share class version on the trade date
       ON f.fund_nk = s.isin AND s.trade_date >= f.valid_from AND s.trade_date < f.valid_to
JOIN sim.silver.account a ON a.account_id = s.account_id
JOIN sim.silver.dim_client c ON c.client_id = a.client_id
WHERE s._ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1900-01-01') FROM sim.gold.fct_platform_transaction) - INTERVAL 72 HOUR
ON CONFLICT (transaction_sk) DO UPDATE SET ${FACT_COLS.slice(1).map((c) => `${c} = excluded.${c}`).join(", ")}`);
  const fact = await scalar("SELECT count(*) FROM sim.gold.fct_platform_transaction");

  await q(task, `CREATE OR REPLACE TABLE sim.gold.finance_net_flows_daily AS
SELECT legal_entity, wrapper_code, trade_date,
       round(sum(CASE WHEN amount_gbp > 0 THEN amount_gbp ELSE 0 END), 2) AS gross_inflows_gbp,
       round(-sum(CASE WHEN amount_gbp < 0 THEN amount_gbp ELSE 0 END), 2) AS gross_outflows_gbp,
       round(sum(amount_gbp), 2) AS net_flows_gbp
FROM sim.gold.fct_platform_transaction
WHERE transaction_type IN ('contribution', 'withdrawal', 'fee')   -- Finance's Net Flows definition
GROUP BY ALL`);
  await q(task, `CREATE OR REPLACE TABLE sim.gold.marketing_acquisition_monthly AS
WITH first_open AS (SELECT client_id, min(opened_on) AS first_open FROM sim.silver.account GROUP BY 1),
     new AS (SELECT CAST(date_trunc('month', first_open) AS DATE) AS month, c.channel, count(*) AS new_clients
             FROM first_open JOIN sim.silver.dim_client c USING (client_id)
             WHERE first_open >= DATE '2025-10-01' GROUP BY ALL)
SELECT s.month, s.channel, coalesce(n.new_clients, 0) AS new_clients, s.spend_gbp,
       round(s.spend_gbp / nullif(coalesce(n.new_clients, 0), 0), 2) AS cost_per_acquisition_gbp
FROM sim.silver.marketing_spend s LEFT JOIN new n USING (month, channel)`);
  await q(task, `CREATE OR REPLACE TABLE sim.gold.hr_workforce_monthly AS
WITH m AS (SELECT month, last_day(month) AS month_end, CAST(last_day(month) - INTERVAL 12 MONTH AS DATE) AS window_start FROM (${MONTHS})),
     agg AS (SELECT month, function,
                    sum(CASE WHEN hired_on <= month_end AND (left_on IS NULL OR left_on > month_end) THEN 1 ELSE 0 END) AS headcount,
                    sum(CASE WHEN left_on > window_start AND left_on <= month_end THEN 1 ELSE 0 END) AS leavers
             FROM m CROSS JOIN sim.silver.colleague GROUP BY ALL)
SELECT month, function, headcount,
       CASE WHEN leavers >= 5 THEN leavers END AS leavers_12m,          -- cells under 5 suppressed by contract
       round(CASE WHEN leavers >= 5 THEN leavers END / (headcount + CASE WHEN leavers >= 5 THEN leavers END / 2), 4) AS attrition_rate_12m
FROM agg`);
  await q(task, `CREATE OR REPLACE TABLE sim.gold.executive_kpis_monthly AS
WITH m AS (SELECT month, last_day(month) AS month_end FROM (${MONTHS})),
     flows_to AS (SELECT m.month, a.account_id, a.client_id, a.opening_value, coalesce(sum(t.amount_gbp), 0) AS cum_flows
                  FROM m CROSS JOIN sim.silver.account a
                  LEFT JOIN sim.gold.fct_platform_transaction t ON t.account_id = a.account_id AND t.trade_date <= m.month_end
                  WHERE a.opened_on <= m.month_end GROUP BY ALL),
     aua AS (SELECT month, round(sum(greatest(opening_value + cum_flows, 0)), 0) AS aua_gbp, count(DISTINCT client_id) AS active_clients
             FROM flows_to GROUP BY ALL),
     flows AS (SELECT CAST(date_trunc('month', trade_date) AS DATE) AS month, round(sum(net_flows_gbp), 2) AS net_flows_gbp
               FROM sim.gold.finance_net_flows_daily GROUP BY ALL)
SELECT * FROM aua LEFT JOIN flows USING (month) ORDER BY month`);
  await q(task, `CREATE OR REPLACE TABLE sim.gold.product_journeys_monthly AS
SELECT CAST(date_trunc('month', created_on) AS DATE) AS month, journey_type, count(*) AS requested, count(completed_on) AS completed,
       quantile_disc(date_diff('day', created_on, completed_on), 0.5) AS median_days,
       quantile_disc(date_diff('day', created_on, completed_on), 0.9) AS p90_days,
       round(count(completed_on) / count(*), 4) AS completion_rate
FROM sim.silver.service_journey GROUP BY ALL`);
  return { fact_rows: fact, fact_upserted: fact - before >= 0 ? `${fmt(fact - before)} new` : "", products: 5 };
}

// ---- Contracts: the compiled checks from src/nexus/contracts.py ----------------------------
function checkToDuck(sql) {
  return sql.replace(/\bnexus_(bronze|silver|gold)\./g, "sim.$1.").replace(/trunc\((\w+), 'MM'\)/g, "CAST(date_trunc('month', $1) AS DATE)");
}
async function runContracts(task) {
  const results = [];
  for (const c of site.contracts) {
    let passed = 0;
    const failing = [];
    for (const r of c.results) {
      let n;
      if (r.kind === "privacy_columns") {
        const cols = (await conn.query(`DESCRIBE sim.gold.${c.product}`)).toArray().map((x) => x.toJSON().column_name);
        n = cols.filter((x) => ["client_id", "email", "national_insurance_no", "colleague_id", "full_name"].includes(x)).length;
        state[task].sql.push(`-- ${c.product}: ${r.check}\nDESCRIBE sim.gold.${c.product}`);
      } else {
        const sql = checkToDuck(r.sql);
        n = (await q(task, `-- ${c.product}: ${r.check}\n${sql}`)).numRows;
        if (n) state[task].lastFailSql = sql;
      }
      if (n === 0) passed++; else failing.push({ check: r.check, rows: n });
    }
    results.push({ product: c.product, department: c.department, owner: c.owner, passed, total: c.results.length, failing });
  }
  return results;
}

// ---- the run --------------------------------------------------------------------------------
let running = false;
async function run() {
  if (running) return;
  running = true;
  $("#run").disabled = true;
  const on = Object.fromEntries(SCENARIOS.map((s) => [s.key, $(`#sc-${s.key}`).checked]));
  for (const k of Object.keys(state)) delete state[k];
  $("#console").innerHTML = ""; $("#console").dataset.started = "1"; $("#outcome").innerHTML = "";
  await conn.query("DETACH DATABASE IF EXISTS sim");
  await conn.query("ATTACH ':memory:' AS sim");
  for (const s of ["bronze", "silver", "gold"]) await conn.query(`CREATE SCHEMA sim.${s}`);
  await conn.query("CREATE SCHEMA IF NOT EXISTS raw");
  renderDag();
  log(`Run started with: ${SCENARIOS.filter((s) => on[s.key]).map((s) => s.label).join(" · ") || "no scenarios"}`);
  const incidents = [], notifications = [];
  const feeds = site.sources.map((s) => s.feed);
  let landing;

  try {
    landing = await step("extract", async (st) => {
      const out = {};
      let rows = 0, bytes = 0;
      for (const f of [...feeds, "platform_transactions_late"]) {
        let data = await fetchFeed(f);
        if (f === "platform_transactions" && !on.malformed) data = data.filter((r) => !/^[XY]/.test(r.transaction_id));
        if (f === "finance_ledger" && !on.ledger) {
          data = data.map((r) => r.trade_date === "2026-08-14" && r.wrapper_code === "ISA" ? { ...r, ledger_net_flows_gbp: Math.round((r.ledger_net_flows_gbp - 250) * 100) / 100 } : r);
        }
        if (f === "platform_transactions_late" && on.drift) data = data.map(({ amount_gbp, ...r }) => ({ ...r, amount: amount_gbp }));
        out[f] = data;
        rows += data.length; bytes += JSON.stringify(data).length;
      }
      st.metrics = { files: Object.keys(out).length, records: rows, "approx MB": +(bytes / 1e6).toFixed(1) };
      st.summary = `${fmt(rows)} records`;
      log(`Extract: fetched ${Object.keys(out).length} landing files, ${fmt(rows)} records`, "ok");
      return out;
    });

    await step("bronze", async (st) => {
      for (const s of site.sources) {
        await loadRaw("bronze", s.feed, landing[s.feed]);
        const m = await mergeBronze("bronze", s, s.feed);
        if (s.feed === "platform_transactions") Object.assign(st.metrics, { "transactions in bronze": m.rows, inserted: m.inserted });
      }
      st.summary = `${fmt(st.metrics["transactions in bronze"])} transactions`;
      st.sample = await sampleOf("SELECT transaction_id, transaction_type, amount_gbp, trade_date, cdc_lsn, op FROM sim.bronze.platform_transaction ORDER BY cdc_lsn");
      log(`Load: MERGE into bronze, ${fmt(st.metrics["transactions in bronze"])} unique transactions from ${fmt(landing.platform_transactions.length)} records (in-batch duplicates collapsed)`, "ok");
    });

    if (on.replay) {
      await step("replay", async (st) => {
        let inserted = 0, updated = 0;
        for (const s of site.sources) {
          const m = await mergeBronze("replay", s, s.feed);
          inserted += m.inserted; updated += m.updated;
        }
        st.metrics = { "rows inserted": inserted, "rows updated": updated };
        st.summary = `${inserted} inserted · ${updated} updated`;
        log(`Replay: every feed loaded again → ${inserted} inserted, ${updated} updated. Idempotent.`, inserted + updated ? "bad" : "ok");
      });
    } else skip("replay", "scenario off");

    await step("quality", async (st) => {
      const r = await buildSilver("quality");
      st.metrics = { "valid rows": r.valid, quarantined: r.quarantined, reasons: r.reasons || "none" };
      st.summary = `${fmt(r.valid)} valid · ${r.quarantined} quarantined`;
      st.sample = await sampleOf("SELECT transaction_id, isin, transaction_type, _failed_rules FROM sim.silver.platform_transaction_quarantine");
      log(`Quality: ${fmt(r.valid)} rows pass the source contract; ${r.quarantined} quarantined${r.reasons ? ` (${r.reasons})` : ""}`, r.quarantined ? "warn" : "ok");
    });

    await step("silver", async (st) => {
      const r = await buildConformed("silver");
      st.metrics = { "share-class versions": r.scd2_versions, "repriced funds (SCD2)": r.repriced_funds };
      st.summary = `${r.repriced_funds} repriced funds`;
      st.sample = await sampleOf("SELECT fund_nk, ongoing_charge_pct, valid_from, valid_to FROM sim.silver.dim_fund_share_class ORDER BY fund_nk, valid_from");
      log(`Silver: SCD2 fund dimension (${r.scd2_versions} versions, ${r.repriced_funds} repriced) and conformed tables`, "ok");
    });

    await step("gold", async (st) => {
      const r = await buildGold("gold");
      st.metrics = { "fact rows": r.fact_rows, "products built": r.products };
      st.summary = `${fmt(r.fact_rows)} facts · 5 products`;
      st.sample = await sampleOf("SELECT trade_date, wrapper_code, legal_entity, net_flows_gbp FROM sim.gold.finance_net_flows_daily ORDER BY trade_date DESC");
      await conn.query("CREATE TABLE sim.gold.finance_net_flows_daily_as_issued AS SELECT * FROM sim.gold.finance_net_flows_daily");
      log(`Gold: fact merged (${fmt(r.fact_rows)} rows, point-in-time fund join) and 5 department products published`, "ok");
    });

    const first = await step("check1", async (st) => {
      const res = await runContracts("check1");
      const p = res.reduce((a, c) => a + c.passed, 0), t = res.reduce((a, c) => a + c.total, 0);
      st.metrics = { passed: p, total: t };
      st.summary = `${p}/${t} passed`;
      for (const c of res) for (const f of c.failing) log(`Contract: ${c.department} · ${f.check} → ${f.rows} row(s)`, "warn");
      return res;
    });
    const reconIssued = first.find((c) => c.department === "Finance").failing.find((f) => /reconcile/.test(f.check))?.rows || 0;

    let lateOk = false;
    if (on.late) {
      try {
        await step("late", async (st) => {
          await loadRaw("late", "platform_transactions_late", landing.platform_transactions_late);
          const tx = site.sources.find((s) => s.feed === "platform_transactions");
          const m = await mergeBronze("late", tx, "platform_transactions_late");
          st.metrics = { "rows in batch": landing.platform_transactions_late.length, inserted: m.inserted, updated: m.updated };
          st.summary = `${m.updated} corrections applied`;
          st.sample = await sampleOf("SELECT transaction_id, amount_gbp, cdc_lsn, op FROM sim.bronze.platform_transaction WHERE op = 'U'");
          log(`Late batch: ${m.updated} corrections applied by MERGE (higher CDC sequence wins), ${m.inserted} new rows`, "ok");
        });
        lateOk = true;
      } catch (e) {
        log(`Late batch FAILED: ${e.message}`, "bad");
        incidents.push({
          title: "Upstream schema change stopped the late batch",
          severity: "P2: data delayed, nothing wrong published",
          timeline: ["Load detected the change before writing: amount_gbp missing, amount unexpected.",
            "The stream stopped. Bronze, silver and gold stayed on the last good version.",
            "Finance's product stays 'as issued'; the 40 corrections wait in landing.",
            "Fix: a contract change agreed with the platform team (rename mapped in the source contract), then replay the late batch. Replay is safe by design."],
        });
        notifications.push({ to: tx_owner(), subject: "Late corrections batch stopped: schema change on platform_transactions",
          body: "Today's late batch renamed amount_gbp to amount, so the load stopped before writing anything. Published numbers are unchanged and still match the first publish. Can we agree the change as a contract update today? Once merged, I'll replay the batch and confirm the restated Finance figures. Next update at 12:00." });
      }
    } else skip("late", "scenario off");

    if (lateOk) {
      await step("restate", async (st) => {
        await buildSilver("restate");
        await buildConformed("restate");
        await buildGold("restate");
        const changed = await scalar(`SELECT count(*) FROM sim.gold.finance_net_flows_daily n JOIN sim.gold.finance_net_flows_daily_as_issued i
          USING (trade_date, wrapper_code, legal_entity) WHERE n.net_flows_gbp <> i.net_flows_gbp`);
        st.metrics = { "Finance rows restated": changed };
        st.summary = `${changed} rows restated`;
        st.sample = await sampleOf(`SELECT n.trade_date, n.wrapper_code, i.net_flows_gbp AS as_issued, n.net_flows_gbp AS now
          FROM sim.gold.finance_net_flows_daily n JOIN sim.gold.finance_net_flows_daily_as_issued i USING (trade_date, wrapper_code, legal_entity)
          WHERE n.net_flows_gbp <> i.net_flows_gbp ORDER BY abs(n.net_flows_gbp - i.net_flows_gbp) DESC`);
        log(`Restate: silver and gold re-run incrementally; ${changed} Finance rows changed from the issued version`, "ok");
      });
    } else skip("restate", on.late ? "late batch stopped" : "scenario off");

    const final = lateOk ? await step("check2", async (st) => {
      const res = await runContracts("check2");
      const p = res.reduce((a, c) => a + c.passed, 0), t = res.reduce((a, c) => a + c.total, 0);
      st.metrics = { passed: p, total: t };
      st.summary = `${p}/${t} passed`;
      for (const c of res) for (const f of c.failing) log(`Contract: ${c.department} · ${f.check} → ${f.rows} row(s)`, "warn");
      return res;
    }) : (skip("check2", "no restatement"), first);

    await step("notify", async (st) => {
      for (const c of final) for (const f of c.failing) {
        if (/reconcile to finance_ledger/.test(f.check)) {
          const row = (await conn.query(checkToDuck(site.contracts.find((x) => x.product === c.product).results.find((r) => r.check === f.check).sql))).toArray().map((r) => r.toJSON());
          const detail = row.slice(0, 3).map((r) => `${r.trade_date ? new Date(r.trade_date).toISOString().slice(0, 10) : "?"} ${r.wrapper_code}: platform £${r.platform ?? 0}, ledger £${r.ledger ?? 0}`).join("; ");
          const why = on.late && !lateOk
            ? "These days are waiting for the 40 late corrections, which are held back by today's schema change. They should reconcile once the batch is replayed. Until then, please treat the affected days as provisional."
            : "It looks like a ledger posting the platform never received. I've asked the platform team to check the 14 Aug ISA feed.";
          notifications.push({ to: c.owner, subject: `Net flows don't reconcile to the ledger on ${f.rows} day(s)`,
            body: `${detail}${f.rows > 3 ? ` and ${f.rows - 3} more` : ""}. The product is flagged as not reconciled; everything else in it passed. ${why} I'll update you by 12:00. If the board pack needs the number before then, use it with this caveat.` });
        } else {
          notifications.push({ to: c.owner, subject: `${c.product}: ${f.check} failed`, body: `${f.rows} row(s) failed this check on today's run. The product is flagged; I'll send the root cause and fix today.` });
        }
      }
      st.metrics = { messages: notifications.length };
      st.summary = notifications.length ? `${notifications.length} owner(s) told` : "nothing to report";
      log(notifications.length ? `Notify: ${notifications.length} message(s) to data owners, written from the contract` : "Notify: all contracts green, nothing to send", notifications.length ? "warn" : "ok");
    });

    renderOutcome({ on, first, final, reconIssued, incidents, notifications });
    log("Run finished.", "ok");
  } catch (e) {
    log(`Run stopped: ${e.message}`, "bad");
  } finally {
    running = false;
    $("#run").disabled = false;
    $("#run").textContent = "Run again";
  }
}
function tx_owner() { return site.sources.find((s) => s.feed === "platform_transactions").owner; }

function renderOutcome({ on, first, final, reconIssued, incidents, notifications }) {
  const spark = site.run;
  const bronze = state.bronze?.metrics["transactions in bronze"], q = state.quality?.metrics;
  const defaults = on.replay && on.malformed && on.late && !on.drift && on.ledger;
  const p = final.reduce((a, c) => a + c.passed, 0), t = final.reduce((a, c) => a + c.total, 0);
  const sparkChecks = site.contracts.reduce((a, c) => a + c.passed, 0) + "/" + site.contracts.reduce((a, c) => a + c.total, 0);
  const cmp = [
    ["Transactions in bronze", fmt(spark.bronze.platform_transactions), fmt(bronze)],
    ["Rows quarantined", spark.silver.quarantine, q?.quarantined],
    ["Valid rows in silver", fmt(spark.silver.platform_transaction), fmt(q?.["valid rows"])],
    ["Contract checks passed (final)", sparkChecks, `${p}/${t}`],
  ];
  $("#outcome").innerHTML = `<div class="out-grid">
    <div class="out-card"><h3>Browser vs Spark</h3><table>${cmp.map(([k, s, b]) => `<tr><td>${k}</td><td>${s} · <b class="${String(s) === String(b) ? "pass" : "fail"}">${b}</b></td></tr>`).join("")}</table>
      <p class="muted" style="font-size:.8rem">${defaults ? "Default scenarios: the browser run should match Spark (left) exactly." : "You changed the scenarios, so differences from the Spark run (left) are expected."}</p></div>
    <div class="out-card"><h3>Contracts by department</h3><table>${final.map((c) => `<tr><td>${c.department}</td><td class="${c.failing.length ? "fail" : "pass"}">${c.passed}/${c.total}</td></tr>`).join("")}</table>
      <p class="muted" style="font-size:.8rem">Finance reconciliation as issued: ${reconIssued} day(s) out. ${state.restate?.status === "success" ? `After the late corrections: ${final.find((c) => c.department === "Finance").failing.find((f) => /reconcile/.test(f.check))?.rows || 0}.` : ""}</p></div>
    <div class="out-card ${incidents.length ? "note-card incident" : "note-card"}"><h3>${incidents.length ? "Incident" : "No incident"}</h3>${incidents.length ? incidents.map((i) =>
      `<b>${esc(i.title)}</b><br><span class="muted">${esc(i.severity)}</span><ol class="timeline">${i.timeline.map((x) => `<li>${esc(x)}</li>`).join("")}</ol>`).join("")
      : `<p>Nothing published was wrong. ${on.replay ? "The replay changed 0 rows. " : ""}${q?.quarantined ? `${q.quarantined} bad rows are quarantined with an owner, not in any product.` : ""}</p>`}</div>
  </div>
  ${notifications.length ? `<h3 style="margin-top:1.4rem">What the owners receive</h3>${notifications.map((n) => `<div class="msg"><small>To ${esc(n.to)}</small><b>${esc(n.subject)}</b><p style="margin:.3rem 0 0">${esc(n.body)}</p></div>`).join("")}
  <p class="muted">How to handle the people side of this is in the <a href="#" data-goto="people" style="color:var(--green);font-weight:700">Stakeholder week</a> simulation.</p>` : ""}`;
  const go = $("#outcome [data-goto]");
  if (go) go.onclick = (e) => { e.preventDefault(); showTab("people"); window.scrollTo({ top: $(".sim-tabs").offsetTop - 80, behavior: "smooth" }); };
}

$("#scenarios").insertAdjacentHTML("beforeend", SCENARIOS.map((s) =>
  `<label><input type="checkbox" id="sc-${s.key}" ${s.on ? "checked" : ""}><span><b>${esc(s.label)}</b><small>${esc(s.hint)}</small></span></label>`).join(""));
$("#sc-drift").addEventListener("change", (e) => { if (e.target.checked) $("#sc-late").checked = true; });
$("#sc-late").addEventListener("change", (e) => { if (!e.target.checked) $("#sc-drift").checked = false; });
renderDag();
$("#engine").textContent = "Loading DuckDB-WASM…";
boot().then((v) => {
  $("#engine").textContent = `DuckDB ${v} in your browser`;
  $("#run").disabled = false;
  $("#run").textContent = "Run the pipeline";
  $("#run").onclick = run;
}).catch((e) => { $("#engine").textContent = `Could not start DuckDB: ${e.message}`; });

// =============================================================================================
// 2. STAKEHOLDER WEEK
// =============================================================================================
const DEPTS = ["Finance", "Marketing", "HR", "Executive", "Product"];
const START = { Finance: 60, Marketing: 60, HR: 60, Executive: 60, Product: 60, focus: 70, risk: 30 };

// Each option: d = changes {Finance:+10, focus:-5, risk:+10 …}. best marks the answer I'd give.
const WEEK = [
  { when: "Monday 07:10", from: "Finance", title: "Net flows don't match the ledger",
    msg: "Month-end flows are £250 out against the ledger for ISAs on 14 August. The board pack goes out on Thursday.",
    options: [
      { t: "Adjust the number in the report so it matches the ledger.", d: { Finance: 5, risk: 25, Executive: -5 },
        f: "It makes the problem go away for a day, and puts an untraceable number in a board pack. When it resurfaces, trust in every number drops." },
      { t: "Reply within the hour: which day and wrapper, what the product shows, that it's flagged, and when you'll update next. Then investigate.", best: true, d: { Finance: 15, risk: -10, focus: -5 },
        f: "The contract check already found the exact row, so Finance hears it from me first, with the detail and a time for the next update. No surprises is the rule I hold with every department." },
      { t: "Say nothing until you have the root cause.", d: { Finance: -15, risk: 10 },
        f: "Silence is the worst option. Finance will find out from the board pack, and the question becomes \"what else don't we know?\"" },
      { t: "Ask Finance to raise a ticket.", d: { Finance: -10 },
        f: "Process has its place, but the platform found this problem, so the platform should report it." },
    ] },
  { when: "Monday 10:00", from: "Marketing", title: "CPA by channel, today",
    msg: "We need last month's cost per acquisition by channel this afternoon to decide where to put next month's budget.",
    options: [
      { t: "Write them a custom SQL query and send the results.", d: { Marketing: 10, focus: -10, risk: 5 },
        f: "Fast once, but it creates a second definition of CPA, and the same request comes back next month." },
      { t: "Show them the certified CPA measure in self-service, with a ten-minute walkthrough.", best: true, d: { Marketing: 12, focus: 5 },
        f: "This is delegation in the First Light triage: urgent but already answered. They get it in minutes, and next month they don't need us." },
      { t: "Put it on next sprint's backlog.", d: { Marketing: -15 },
        f: "The decision is today. A backlog item next sprint misses it." },
      { t: "Build a new CPA dashboard.", d: { Marketing: 5, focus: -15 },
        f: "Too much for the question, and it duplicates the certified measure." },
    ] },
  { when: "Tuesday 09:30", from: "Executive", title: "Two decks, two numbers",
    msg: "Finance's deck and the Exec MI deck show different net flows for September. Which one is right?",
    options: [
      { t: "Tell them to use Finance's number.", d: { Executive: 0, risk: 10 },
        f: "It answers today and leaves the cause in place, so the decks will disagree again." },
      { t: "Trace both with lineage, find which deck skipped the metric layer, fix that deck, and send a short written note.", best: true, d: { Executive: 15, Finance: 5, risk: -10, focus: -5 },
        f: "One definition, held in the platform. The Exec product's contract even reconciles to Finance's, so the difference had to come from outside it." },
      { t: "Average the two numbers.", d: { Executive: -10, risk: 15 },
        f: "Now there are three numbers." },
    ] },
  { when: "Tuesday 14:00", from: "HR", title: "Leavers by name",
    msg: "Can you give me the names of Technology leavers this year? I want to look for patterns.",
    options: [
      { t: "Export them from the colleague table.", d: { HR: 10, risk: 30 },
        f: "That's personal data leaving a governed system for an analysis that doesn't need names." },
      { t: "Offer the aggregate product (attrition by function, cells under 5 suppressed), and route record-level questions through HR's own system and the DPO.", best: true, d: { HR: 10, risk: -10 },
        f: "Patterns don't need names. The contract already suppresses small cells, and the DPO decides record-level access, not engineering." },
      { t: "Refuse.", d: { HR: -15 },
        f: "The need is real. A plain no without an alternative loses the relationship." },
    ] },
  { when: "Wednesday 08:00", from: "Product", title: "The late batch didn't load",
    msg: "Overnight the platform renamed amount_gbp to amount. The load stopped. (Try it: tick 'Upstream schema change' in the ELT simulation.)",
    options: [
      { t: "Hotfix the code to accept the new name and rerun.", d: { focus: -5, risk: 15 },
        f: "It works until the next unannounced change. The contract exists so the interface changes on purpose." },
      { t: "Keep published numbers on the last good version, tell Finance and the platform owner, agree the change as a contract update, then replay the batch.", best: true, d: { Product: 10, Finance: 10, risk: -10, focus: -5 },
        f: "The pipeline did its job by stopping before writing. Replay is safe by design, so the fix is a reviewed change and one re-run." },
      { t: "Wait for the platform team to change it back.", d: { Finance: -10, risk: 5 },
        f: "Corrections stay unapplied with nobody owning the fix." },
    ] },
  { when: "Wednesday 11:00", from: "Product", title: "A new field by Friday",
    msg: "Can we add 'switch reason' to the journeys dashboard by Friday?",
    options: [
      { t: "Add it straight to gold.", d: { Product: 10, risk: 10 },
        f: "Quick, but it skips the data owner and the classification check on a new field." },
      { t: "Yes: as a contract change. Product agrees the definition in the YAML, the PR shows the blast radius, and it ships Thursday.", best: true, d: { Product: 12, risk: -5 },
        f: "The definition goes into the contract first and the dashboard second. It's still on time." },
      { t: "No, not this quarter.", d: { Product: -15 },
        f: "Small, low-risk and wanted. Refusing it costs trust for nothing." },
    ] },
  { when: "Thursday 09:00", from: "Executive", title: "Two engineers off sick",
    msg: "Two of five engineers are off this week. You have the board pack fix, the contract change and three smaller requests.",
    options: [
      { t: "Everyone works on everything to keep all dates.", d: { focus: -20, risk: 10 },
        f: "Context switching means everything finishes late." },
      { t: "Cut scope: one primary commitment per engineer, board pack first, and tell the owners of the deferred items the new dates today.", best: true, d: { focus: 15, Executive: 5, Marketing: -3 },
        f: "First Light: when capacity drops, reduce scope and keep quality. Deferred owners hear it from me with a date." },
      { t: "Ask the team to work the weekend.", d: { focus: -10 },
        f: "It might work once, and it costs more than it saves." },
    ] },
  { when: "Thursday 16:00", from: "Executive", title: "Can the AI answer clients?",
    msg: "Could the new AI assistant answer client questions about their portfolios directly?",
    options: [
      { t: "Yes: connect it to silver with a service account.", d: { Executive: 10, risk: 35 },
        f: "A broadly privileged service account bypasses every mask and row filter, and client-facing answers could become advice." },
      { t: "Start internal: governed tools only, the caller's own permissions, citations on every answer, no advice; review client use with Compliance.", best: true, d: { Executive: 8, risk: -5 },
        f: "Agents get governed tools, not credentials. Client-facing use is a Consumer Duty and advice question, so Compliance decides." },
      { t: "No AI near client data.", d: { Executive: -10 },
        f: "Safe but closes a useful door. There's a governed way to start." },
    ] },
  { when: "Friday 10:00", from: "Finance", title: "What did we say on 1 October?",
    msg: "An auditor wants the net flows figure exactly as we issued it, before the late corrections.",
    options: [
      { t: "Recompute it from today's data.", d: { Finance: -10, risk: 15 },
        f: "Today's data includes the corrections, so it isn't what was issued." },
      { t: "Time travel to the issued version, and attach the restatement diff with the reason for each change.", best: true, d: { Finance: 15, risk: -10 },
        f: "Delta keeps both versions. Try it in the lakehouse: notebook 03 runs exactly this." },
      { t: "Explain we can't reproduce it.", d: { Finance: -20, risk: 10 },
        f: "In a regulated firm, 'we can't say what we published' is a finding." },
    ] },
  { when: "Friday 15:00", from: "Marketing", title: "A monthly 40-page report",
    msg: "Could we get this 40-page report every month from now on?",
    options: [
      { t: "Yes, schedule it.", d: { Marketing: 5, focus: -10 },
        f: "Forty pages nobody acts on is ongoing cost with no decision behind it." },
      { t: "Ask which decisions it supports; agree three KPIs in self-service and retire the rest.", best: true, d: { Marketing: 8, focus: 8 },
        f: "Start from the decision. At Lifepal this is how ad-hoc requests fell from 214 to 86 a quarter." },
      { t: "Decline.", d: { Marketing: -10 },
        f: "A reason and an alternative work better than a no." },
    ] },
];

let game;
function newGame() { game = { i: 0, score: { ...START }, picks: [] }; renderMeters(); renderScenario(); }
function clamp(v) { return Math.max(0, Math.min(100, v)); }
function renderMeters(delta = {}) {
  const items = [...DEPTS.map((d) => [d, `${d} trust`]), ["focus", "Team focus"], ["risk", "Platform risk"]];
  $("#meters").innerHTML = items.map(([k, label]) => {
    const v = game.score[k], dv = delta[k];
    const good = k === "risk" ? dv < 0 : dv > 0;
    return `<div class="meter ${k === "risk" ? "risk" : ""}"><small>${label}</small><b>${v}</b>${dv ? `<span class="delta ${good ? "up" : "down"}">${dv > 0 ? "+" : ""}${dv}</span>` : ""}
      <div class="bar"><i style="width:${v}%"></i></div></div>`;
  }).join("");
}
function renderScenario() {
  if (game.i >= WEEK.length) return renderSummary();
  const s = WEEK[game.i];
  $("#scenario").innerHTML = `<div class="when">${s.when} · ${game.i + 1} of ${WEEK.length}</div>
    <h3><span class="from">${s.from}</span>${esc(s.title)}</h3><blockquote>${esc(s.msg)}</blockquote>
    <div class="options">${s.options.map((o, k) => `<button data-k="${k}">${esc(o.t)}</button>`).join("")}</div><div id="fb"></div>`;
  document.querySelectorAll(".options button").forEach((b) => b.onclick = () => choose(+b.dataset.k));
}
function choose(k) {
  const s = WEEK[game.i], o = s.options[k];
  for (const [key, dv] of Object.entries(o.d)) game.score[key] = clamp(game.score[key] + dv);
  game.picks.push({ i: game.i, k, best: !!o.best });
  document.querySelectorAll(".options button").forEach((b) => {
    b.disabled = true;
    if (+b.dataset.k === k) b.classList.add("picked");
    if (s.options[+b.dataset.k].best) b.classList.add("best");
  });
  const best = s.options.find((x) => x.best);
  $("#fb").innerHTML = `<div class="feedback"><h4>${o.best ? "That's what I'd do." : "What I'd do instead"}</h4>
    <p>${esc(o.f)}</p>${o.best ? "" : `<p><b>My pick:</b> ${esc(best.t)} ${esc(best.f)}</p>`}</div>
    <button class="btn-run next" id="next">${game.i + 1 < WEEK.length ? "Next request →" : "See the week →"}</button>`;
  renderMeters(o.d);
  $("#next").onclick = () => { game.i++; renderMeters(); renderScenario(); };
}
function renderSummary() {
  const matched = game.picks.filter((p) => p.best).length;
  const trust = Math.round(DEPTS.reduce((a, d) => a + game.score[d], 0) / DEPTS.length);
  $("#scenario").innerHTML = `<div class="when">Friday 17:30 · week done</div><h3>${matched} of ${WEEK.length} choices matched mine</h3>
    <div class="summary-grid">
      <div class="out-card"><h3>Where you ended</h3><table>
        <tr><td>Average department trust</td><td>${trust} (from 60)</td></tr>
        <tr><td>Team focus</td><td>${game.score.focus} (from 70)</td></tr>
        <tr><td>Platform risk</td><td>${game.score.risk} (from 30, lower is better)</td></tr></table></div>
      <div class="out-card"><h3>The rules behind my picks</h3><ol class="timeline">
        <li>No surprises: owners hear bad news from me first, in writing, with the next update time.</li>
        <li>Start from the decision, and point to certified measures before building anything.</li>
        <li>Definitions go into the contract first and the report second.</li>
        <li>A no always comes with a reason and an alternative.</li>
        <li>When capacity drops, cut scope and keep quality.</li>
        <li>Controls stay on: personal data, access and regulatory numbers go through the owner and the DPO.</li></ol></div>
    </div>`;
}
$("#restart").onclick = newGame;
newGame();
if (location.hash === "#people") showTab("people");
