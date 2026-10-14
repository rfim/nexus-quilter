// NEXUS Lakehouse: the real Delta tables from the PySpark run, replayed in the browser.
//
// Delta part: for each table we fetch its _delta_log commits (newline-delimited JSON actions)
// and replay add/remove actions to get the exact set of Parquet files at any version. That gives
// DESCRIBE HISTORY, DESCRIBE DETAIL and VERSION AS OF / TIMESTAMP AS OF straight from the log.
// Query part: parquet-wasm (Rust arrow-rs) decodes each data file a version points to, and
// DuckDB-WASM runs the SQL over the Arrow data. No DuckDB extension is downloaded, so it also
// works on networks that block extensions.duckdb.org.
// Unity Catalog part: three-level names (nexus.<schema>.<table>), grants, column masks and row
// filters, applied according to the persona you "run as". It is a simulation of the behaviour,
// not Databricks itself.
const DUCKDB = "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";
const PARQUET = "https://cdn.jsdelivr.net/npm/parquet-wasm@0.6.1/esm/parquet_wasm.js";
const PARQUET_WASM = "https://cdn.jsdelivr.net/npm/parquet-wasm@0.6.1/esm/parquet_wasm_bg.wasm";
const BASE = new URL("assets/lakehouse/", document.baseURI);
const MAX_ROWS = 200;
const $ = (s, r = document) => r.querySelector(s);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);

// ---- personas: who you are decides grants, masks and row filters ------------------------------
const PERSONAS = [
  { key: "engineer", label: "VIM · data platform engineer", groups: ["grp_platform"], mask: "hash" },
  { key: "dpo", label: "Data protection officer", groups: ["grp_dpo"], mask: "none" },
  { key: "finance", label: "Finance analyst", groups: ["grp_finance"], mask: "redact" },
  { key: "exec", label: "Executive MI (all entities)", groups: ["grp_exec_mi"], mask: "redact" },
  { key: "affluent", label: "Executive MI (Affluent only)", groups: ["grp_exec_mi"], mask: "redact", rowFilter: "affluent" },
  { key: "marketing", label: "Marketing analyst", groups: ["grp_marketing"], mask: "redact" },
  { key: "hr", label: "People analytics (HR)", groups: ["grp_hr"], mask: "redact" },
];
let persona = PERSONAS[0];

function canRead(t, p = persona) {
  const g = new Set(p.groups);
  if (t.layer === "bronze") return g.has("grp_platform");
  if (t.layer === "silver") return g.has("grp_platform") || g.has("grp_dpo");
  if (t.name === "nexus.gold.fct_platform_transaction") return g.has("grp_platform") || g.has("grp_finance") || g.has("grp_dpo");
  return g.has("grp_platform") || g.has("grp_dpo") || t.read.some((r) => g.has(r));
}
function grantsFor(t) {
  if (t.layer === "bronze") return ["grp_platform"];
  if (t.layer === "silver") return ["grp_platform", "grp_dpo"];
  if (t.name === "nexus.gold.fct_platform_transaction") return ["grp_platform", "grp_finance", "grp_dpo"];
  return ["grp_platform", "grp_dpo", ...t.read];
}

// ---- Delta log replay -----------------------------------------------------------------------
const TABLES = new Map();      // "nexus.gold.x" -> table
const loaded = new Map();       // data file path -> DuckDB table holding its rows
let pq;

async function loadTable(t) {
  t.commits = [];
  for (let v = 0; v < t.versions; v++) {
    const text = await (await fetch(new URL(`${t.path}/_delta_log/${String(v).padStart(20, "0")}.json`, BASE))).text();
    t.commits.push({ version: v, actions: text.trim().split("\n").map((l) => JSON.parse(l)) });
  }
  t.current = t.versions - 1;
  return t;
}

function snapshot(t, version = t.current) {
  if (version < 0 || version > t.current) throw new Error(`[DELTA_VERSIONS_NOT_CONTIGUOUS] Cannot time travel ${t.name} to version ${version}. Available versions: [0, ${t.current}].`);
  const files = new Map();
  let meta = null, info = null;
  for (const c of t.commits.slice(0, version + 1)) {
    for (const a of c.actions) {
      if (a.metaData) meta = a.metaData;
      if (a.add) files.set(a.add.path, a.add);
      if (a.remove) files.delete(a.remove.path);
      if (a.commitInfo) info = a.commitInfo;
    }
  }
  const fields = JSON.parse(meta.schemaString).fields;
  return { version, files: [...files.values()], fields, info };
}

function versionAt(t, ts) {
  const when = Date.parse(ts);
  let found = -1;
  for (const c of t.commits) {
    const info = c.actions.find((a) => a.commitInfo)?.commitInfo;
    if (info && info.timestamp <= when) found = c.version;
  }
  if (found < 0) throw new Error(`[DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION] ${ts} is before the first commit of ${t.name}.`);
  return found;
}

const DUCK_TYPES = { string: "VARCHAR", double: "DOUBLE", long: "BIGINT", integer: "INTEGER", date: "DATE",
  timestamp: "TIMESTAMP", boolean: "BOOLEAN", float: "FLOAT", short: "SMALLINT", byte: "TINYINT" };
function duckType(type) {
  if (typeof type === "object" && type.type === "array") return `${duckType(type.elementType)}[]`;
  const m = String(type).match(/^decimal\((\d+),\s*(\d+)\)$/);
  return m ? `DECIMAL(${m[1]},${m[2]})` : DUCK_TYPES[type] || "VARCHAR";
}
function deltaType(type) { return typeof type === "object" ? `array<${type.elementType}>` : type; }

async function loadFile(name) {
  if (!loaded.has(name)) {
    const bytes = new Uint8Array(await (await fetch(new URL(name, BASE))).arrayBuffer());
    const table = `f${loaded.size}`;
    await conn.insertArrowFromIPCStream(pq.readParquet(bytes).intoIPCStream(), { name: table, schema: "main", create: true });
    loaded.set(name, table);
  }
  return loaded.get(name);
}

// A version is the union of the data files its log says are active
async function scan(t, version) {
  const snap = snapshot(t, version);
  if (!snap.files.length) {
    return `(SELECT ${snap.fields.map((f) => `NULL::${duckType(f.type)} AS "${f.name}"`).join(", ")} WHERE false)`;
  }
  const parts = [];
  for (const f of snap.files) parts.push(`SELECT * FROM main.${await loadFile(`${t.path}/${f.path}`)}`);
  return `(${parts.join(" UNION ALL BY NAME ")})`;
}

// Unity Catalog: column masks and row filters wrap the scan
async function governed(t, version) {
  const snap = snapshot(t, version);
  const src = await scan(t, version);
  const pii = Object.keys(t.pii || {});
  const replace = pii.filter(() => persona.mask !== "none").map((c) => {
    const f = snap.fields.find((x) => x.name === c);
    if (!f) return null;
    if (f.type === "boolean") return `NULL::BOOLEAN AS "${c}"`;
    return persona.mask === "hash" ? `sha256(lower(trim("${c}"))) AS "${c}"` : `'***' AS "${c}"`;
  }).filter(Boolean);
  const hasEntity = snap.fields.some((f) => f.name === "legal_entity");
  const where = persona.rowFilter && hasEntity ? ` WHERE legal_entity = '${persona.rowFilter}'` : "";
  return `(SELECT *${replace.length ? ` REPLACE (${replace.join(", ")})` : ""} FROM ${src}${where})`;
}

// ---- DuckDB ---------------------------------------------------------------------------------
let duck, db, conn;
async function boot() {
  setEngine("Loading DuckDB-WASM and parquet-wasm…");
  pq = await import(PARQUET);
  await pq.default(PARQUET_WASM);
  duck = await import(DUCKDB);
  const bundle = await duck.selectBundle(duck.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  db = new duck.AsyncDuckDB(new duck.VoidLogger(), new Worker(workerUrl));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  await db.open({ query: { castBigIntToDouble: true, castDecimalToDouble: true } });
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();
  await conn.query("ATTACH ':memory:' AS nexus");
  for (const s of ["bronze", "silver", "gold"]) await conn.query(`CREATE SCHEMA IF NOT EXISTS nexus.${s}`);

  setEngine("Replaying Delta logs…");
  const cat = await (await fetch(new URL("catalog.json", BASE))).json();
  await Promise.all(cat.tables.map(async (t) => TABLES.set(t.name, await loadTable(t))));
  await refreshViews();
  const v = (await conn.query("SELECT version() AS v")).toArray()[0].v;
  setEngine(`DuckDB ${v} · ${TABLES.size} Delta tables`);
}

async function refreshViews() {
  for (const t of TABLES.values()) {
    await conn.query(`CREATE OR REPLACE VIEW ${t.name} AS SELECT * FROM ${await governed(t, t.current)}`);
  }
}

function setEngine(s) { $("#engine").textContent = s; }

// ---- SQL: Databricks / Delta syntax on top of DuckDB ----------------------------------------
const REF = /\bnexus\.(bronze|silver|gold)\.(\w+)(?:\s+(?:VERSION\s+AS\s+OF\s+(\d+)|TIMESTAMP\s+AS\s+OF\s+'([^']+)')|@v(\d+))?/gi;

function table(name) {
  const t = TABLES.get(name.toLowerCase());
  if (!t) throw new Error(`[TABLE_OR_VIEW_NOT_FOUND] The table or view \`${name}\` cannot be found.`);
  return t;
}
function checkGrant(t) {
  if (!canRead(t)) throw new Error(`[INSUFFICIENT_PERMISSIONS] Insufficient privileges:\nUser '${persona.label}' (${persona.groups.join(", ")}) does not have SELECT on Table '${t.name}'.\nGranted to: ${grantsFor(t).join(", ")}`);
}

async function special(sql) {
  let m;
  if ((m = sql.match(/^DESCRIBE\s+HISTORY\s+([\w.]+)$/i))) {
    const t = table(m[1]);
    return { columns: ["version", "timestamp", "operation", "summary", "operationMetrics", "engineInfo"],
      rows: [...t.commits].reverse().map((c) => {
        const i = c.actions.find((a) => a.commitInfo).commitInfo, mm = i.operationMetrics || {};
        const summary = i.operation === "MERGE"
          ? `inserted ${mm.numTargetRowsInserted ?? 0} · updated ${mm.numTargetRowsUpdated ?? 0} · deleted ${mm.numTargetRowsDeleted ?? 0} · source rows ${mm.numSourceRows ?? "?"}`
          : `rows written ${mm.numOutputRows ?? "?"} · files ${mm.numFiles ?? "?"}`;
        return [c.version, new Date(i.timestamp).toISOString().replace("T", " ").slice(0, 19), i.operation,
          summary, JSON.stringify(mm), i.engineInfo];
      }) };
  }
  if ((m = sql.match(/^DESCRIBE\s+DETAIL\s+([\w.]+)$/i))) {
    const t = table(m[1]), s = snapshot(t);
    return { columns: ["format", "name", "location", "currentVersion", "numFiles", "sizeInBytes", "owner", "comment"],
      rows: [["delta", t.name, `assets/lakehouse/${t.path}`, t.current, s.files.length,
        s.files.reduce((a, f) => a + f.size, 0), t.owner, t.comment]] };
  }
  if ((m = sql.match(/^DESCRIBE\s+(?:TABLE\s+)?([\w.]+)$/i))) {
    const t = table(m[1]);
    return { columns: ["col_name", "data_type", "tag"], rows: snapshot(t).fields.map((f) => [f.name, deltaType(f.type), t.pii?.[f.name] ? `pii: ${t.pii[f.name]}` : null]) };
  }
  if ((m = sql.match(/^SHOW\s+TABLES\s+IN\s+nexus\.(\w+)$/i))) {
    return { columns: ["database", "tableName", "versions", "canSelect"],
      rows: [...TABLES.values()].filter((t) => t.layer === m[1].toLowerCase()).map((t) => [`nexus.${t.layer}`, t.name.split(".")[2], t.versions, canRead(t)]) };
  }
  if ((m = sql.match(/^SHOW\s+GRANTS\s+ON\s+(?:TABLE\s+)?([\w.]+)$/i))) {
    const t = table(m[1]);
    return { columns: ["principal", "actionType", "objectType", "objectKey"], rows: grantsFor(t).map((g) => [g, "SELECT", "TABLE", t.name]) };
  }
  if (/^SELECT\s+current_user\(\)$/i.test(sql)) return { columns: ["current_user()"], rows: [[persona.label]] };
  return null;
}

async function rewrite(sql) {
  const parts = [];
  let last = 0;
  for (const m of sql.matchAll(REF)) {
    const t = table(`nexus.${m[1]}.${m[2]}`);
    checkGrant(t);
    parts.push(sql.slice(last, m.index));
    const v = m[3] ?? m[5];
    if (v != null) parts.push(await governed(t, Number(v)));
    else if (m[4]) parts.push(await governed(t, versionAt(t, m[4])));
    else parts.push(m[0]);
    last = m.index + m[0].length;
  }
  return parts.join("") + sql.slice(last);
}

async function runSQL(text) {
  const statements = text.split(";").map((s) => s.replace(/--[^\n]*/g, "").trim()).filter(Boolean);
  let result = null;
  const t0 = performance.now();
  for (const s of statements) {
    result = (await special(s)) ?? arrowRows(await conn.query(await rewrite(s)));
  }
  return { ...result, ms: Math.round(performance.now() - t0) };
}

function arrowRows(tbl) {
  const fields = tbl.schema.fields;
  const fmt = fields.map((f) => {
    const ty = String(f.type);
    if (/^Date/.test(ty)) return (v) => v == null ? null : new Date(v).toISOString().slice(0, 10);
    if (/^Timestamp/.test(ty)) {
      const div = [1e-3, 1, 1e3, 1e6][f.type.unit] ?? 1;   // seconds, ms, us, ns -> ms
      return (v) => v == null ? null : new Date(Number(v) / div).toISOString().replace("T", " ").slice(0, 19);
    }
    return (v) => v == null ? null : (typeof v === "object" && v.toJSON ? JSON.stringify(v.toJSON()) : v);
  });
  const rows = [];
  for (const r of tbl) {
    if (rows.length >= MAX_ROWS) break;
    const o = r.toJSON();
    rows.push(fields.map((f, i) => fmt[i](o[f.name])));
  }
  return { columns: fields.map((f) => f.name), rows, total: tbl.numRows };
}

function renderResult(el, res, err) {
  if (err) { el.innerHTML = `<div class="err">${esc(err.message || err)}</div>`; return; }
  const head = `<tr>${res.columns.map((c) => `<th>${esc(c)}</th>`).join("")}</tr>`;
  const body = res.rows.map((r) => `<tr>${r.map((v) => v == null ? '<td class="null">null</td>'
    : `<td class="${typeof v === "number" ? "num" : ""}">${esc(typeof v === "number" ? (Number.isInteger(v) ? v : +v.toFixed(4)) : v)}</td>`).join("")}</tr>`).join("");
  const n = res.total ?? res.rows.length;
  el.innerHTML = `<div class="meta">${n.toLocaleString("en-GB")} row${n === 1 ? "" : "s"}${n > res.rows.length ? ` · showing ${res.rows.length}` : ""} · ${res.ms} ms · run as ${esc(persona.label)}</div><table>${head}${body}</table>`;
}

// ---- notebooks ------------------------------------------------------------------------------
const md = (text) => ({ md: text });
const sql = (text) => ({ sql: text.trim() });
const NOTEBOOKS = [
  { key: "explore", title: "01 · Explore the lakehouse",
    intro: "These are the Delta tables the pipeline wrote, under one catalog called nexus with bronze, silver and gold schemas, the same three-level names Unity Catalog uses. Run any cell, or edit it first.",
    cells: [
      sql("SHOW TABLES IN nexus.gold"),
      sql(`-- Finance's product, as published
SELECT trade_date, wrapper_code, legal_entity, gross_inflows_gbp, gross_outflows_gbp, net_flows_gbp
FROM nexus.gold.finance_net_flows_daily
ORDER BY trade_date DESC, wrapper_code
LIMIT 20`),
      sql("DESCRIBE DETAIL nexus.gold.fct_platform_transaction"),
      sql(`-- Net flows by month and wrapper, straight from the fact table
SELECT date_trunc('month', trade_date) AS month, wrapper_code,
       round(sum(amount_gbp) FILTER (WHERE transaction_type IN ('contribution','withdrawal','fee')), 2) AS net_flows_gbp
FROM nexus.gold.fct_platform_transaction
GROUP BY ALL ORDER BY month, wrapper_code`),
    ] },
  { key: "merge", title: "02 · MERGE and safe replay",
    intro: "Bronze is written by MERGE, never by INSERT. The run landed every feed, then replayed it, then received a late batch of corrections. The Delta history shows all three.",
    cells: [
      sql("DESCRIBE HISTORY nexus.bronze.platform_transaction"),
      md("Version 1 inserted every transaction. Version 2 is the replay: the same records again, with <b>0 rows inserted and 0 updated</b>. Version 3 is the late batch of corrections, applied because their CDC sequence is higher."),
      sql(`-- Row count at each version: the replay changed nothing
SELECT 1 AS version, count(*) AS rows FROM nexus.bronze.platform_transaction VERSION AS OF 1
UNION ALL SELECT 2, count(*) FROM nexus.bronze.platform_transaction VERSION AS OF 2
UNION ALL SELECT 3, count(*) FROM nexus.bronze.platform_transaction VERSION AS OF 3`),
      sql(`-- The corrected rows, before and after the late batch
SELECT b.transaction_id, b.transaction_type, b.amount_gbp AS before_gbp, a.amount_gbp AS after_gbp,
       b.cdc_lsn AS lsn_before, a.cdc_lsn AS lsn_after
FROM nexus.bronze.platform_transaction VERSION AS OF 2 b
JOIN nexus.bronze.platform_transaction a USING (transaction_id)
WHERE a.op = 'U'
ORDER BY transaction_id
LIMIT 15`),
    ] },
  { key: "timetravel", title: "03 · Time travel: as issued vs now",
    intro: "Finance's product was published, then restated when the late corrections arrived. Time travel answers the question a client or auditor asks: what did the number say when we issued it?",
    cells: [
      sql("DESCRIBE HISTORY nexus.gold.finance_net_flows_daily"),
      sql(`-- Days whose published net flows changed after the late corrections
SELECT n.trade_date, n.wrapper_code, n.legal_entity,
       i.net_flows_gbp AS as_issued, n.net_flows_gbp AS now,
       round(n.net_flows_gbp - i.net_flows_gbp, 2) AS restated_by
FROM nexus.gold.finance_net_flows_daily n
JOIN nexus.gold.finance_net_flows_daily VERSION AS OF 0 i
  USING (trade_date, wrapper_code, legal_entity)
WHERE n.net_flows_gbp <> i.net_flows_gbp
ORDER BY abs(n.net_flows_gbp - i.net_flows_gbp) DESC`),
      sql(`-- Reconciliation to the ledger, as issued (version 0) and now
WITH issued AS (SELECT trade_date, wrapper_code, sum(net_flows_gbp) AS p FROM nexus.gold.finance_net_flows_daily VERSION AS OF 0 GROUP BY ALL),
     now AS (SELECT trade_date, wrapper_code, sum(net_flows_gbp) AS p FROM nexus.gold.finance_net_flows_daily GROUP BY ALL),
     l AS (SELECT trade_date, wrapper_code, ledger_net_flows_gbp AS ledger FROM nexus.silver.finance_ledger)
SELECT 'as issued' AS version, count(*) FILTER (WHERE abs(coalesce(p,0) - coalesce(ledger,0)) > 0.01) AS days_not_reconciling
FROM issued FULL JOIN l USING (trade_date, wrapper_code)
UNION ALL
SELECT 'now', count(*) FILTER (WHERE abs(coalesce(p,0) - coalesce(ledger,0)) > 0.01)
FROM now FULL JOIN l USING (trade_date, wrapper_code)`),
      md("After the corrections, one day still doesn't reconcile: the £250 ledger posting the platform never received, planted on purpose. That's the break the Finance contract flags."),
    ] },
  { key: "uc", title: "04 · Unity Catalog: grants, masks, row filters",
    intro: "Change “Run as” at the top right and run these again. Grants come from the department contracts; personal data is masked unless you are the DPO; the Affluent-only MI persona gets a row filter.",
    cells: [
      sql("SELECT current_user()"),
      sql(`-- Personal data: hashed for engineers, clear for the DPO, refused for business users
SELECT client_id, legal_entity, email, national_insurance_no, vulnerable
FROM nexus.silver.dim_client
LIMIT 5`),
      sql("SHOW GRANTS ON nexus.gold.finance_net_flows_daily"),
      sql(`-- Row filter: switch to "Executive MI (Affluent only)" and HNW disappears
SELECT legal_entity, round(sum(net_flows_gbp), 2) AS net_flows_gbp, count(*) AS days
FROM nexus.gold.finance_net_flows_daily
GROUP BY ALL ORDER BY legal_entity`),
      sql(`-- HR's product: aggregates only, small cells already suppressed by the contract
SELECT function, headcount, leavers_12m, attrition_rate_12m
FROM nexus.gold.hr_workforce_monthly
WHERE month = DATE '2026-09-01'
ORDER BY attrition_rate_12m DESC NULLS LAST`),
    ] },
  { key: "contracts", title: "05 · Contract checks", intro: "", cells: [] },
];

let current = NOTEBOOKS[0];

function renderNotebook(nb) {
  current = nb;
  document.querySelectorAll("#tabs button").forEach((b) => b.setAttribute("aria-selected", b.dataset.key === nb.key));
  const view = $("#view");
  view.innerHTML = `<div class="nb-head"><div><h1>${esc(nb.title)}</h1><p>${nb.intro}</p></div><button class="btn" id="runall">Run all</button></div><div id="cells"></div>`;
  const wrap = $("#cells");
  nb.cells.forEach((c, i) => {
    if (c.md) { wrap.insertAdjacentHTML("beforeend", `<div class="cell md"><p>${c.md}</p></div>`); return; }
    const el = document.createElement("div");
    el.className = "cell";
    el.innerHTML = `<div class="cell-bar"><span class="lang">SQL</span><span>Cmd ${i + 1}</span><button class="btn">Run</button></div>
      <textarea spellcheck="false" aria-label="SQL cell ${i + 1}" rows="${Math.min(14, c.sql.split("\n").length + 1)}">${esc(c.sql)}</textarea><div class="out" hidden></div>`;
    const ta = $("textarea", el), out = $(".out", el);
    const go = async () => {
      c.sql = ta.value; out.hidden = false; out.innerHTML = '<div class="meta">Running…</div>';
      try { await ready; renderResult(out, await runSQL(ta.value)); } catch (e) { renderResult(out, null, e); }
    };
    $("button", el).onclick = go;
    ta.addEventListener("keydown", (e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); go(); } });
    el.run = go;
    wrap.append(el);
  });
  $("#runall").onclick = async () => { for (const el of wrap.querySelectorAll(".cell:not(.md)")) await el.run(); };
}

function renderWorkflow(run) {
  document.querySelectorAll("#tabs button").forEach((b) => b.setAttribute("aria-selected", b.dataset.key === "workflow"));
  const total = Math.max(...run.tasks.map((t) => t.start_s + t.duration_s));
  $("#view").innerHTML = `<div class="nb-head"><div><h1>Workflows · nexus-daily</h1>
    <p>The recorded run of <code>scripts/build.sh</code>, the same task order a Databricks job runs (see <code>databricks.yml</code>). Local Spark on two cores, so the durations are for scale, not a benchmark.</p></div></div>
    <div class="gantt">${run.tasks.map((t) => `<div class="row"><span class="ok-dot">${esc(t.label)}</span>
      <div class="track"><div class="bar" style="left:${(t.start_s / total) * 100}%;width:${Math.max(0.6, (t.duration_s / total) * 100)}%"></div></div>
      <span>${t.duration_s.toFixed(1)}s</span></div>`).join("")}</div>`;
}

async function renderTable(t) {
  document.querySelectorAll("#tree button").forEach((b) => b.classList.toggle("on", b.dataset.name === t.name));
  document.querySelectorAll("#tabs button").forEach((b) => b.setAttribute("aria-selected", "false"));
  const s = snapshot(t);
  const allowed = canRead(t);
  $("#view").innerHTML = `<div class="detail"><h1>${esc(t.name)}</h1><p class="sub">${esc(t.comment || "")}</p>
    <div class="grid"><div><small>Format</small><b>Delta · version ${t.current}</b></div><div><small>Owner</small><b>${esc(t.owner)}</b></div>
    <div><small>Files</small><b>${s.files.length} · ${(s.files.reduce((a, f) => a + f.size, 0) / 1024).toFixed(0)} KB</b></div>
    <div><small>Your access</small><b>${allowed ? '<span class="tag ok">SELECT</span>' : '<span class="tag">none</span>'}</b></div></div>
    <div class="cell"><div class="cell-bar"><span class="lang">Columns</span></div><div class="out" id="cols"></div></div>
    <div class="cell"><div class="cell-bar"><span class="lang">History</span></div><div class="out" id="hist"></div></div>
    <div class="cell"><div class="cell-bar"><span class="lang">Sample data</span><span>run as ${esc(persona.label)}</span></div><div class="out" id="sample"></div></div></div>`;
  await ready;
  renderResult($("#cols"), await runSQL(`DESCRIBE ${t.name}`));
  renderResult($("#hist"), await runSQL(`DESCRIBE HISTORY ${t.name}`));
  try { renderResult($("#sample"), await runSQL(`SELECT * FROM ${t.name} LIMIT 20`)); } catch (e) { renderResult($("#sample"), null, e); }
}

function renderTree() {
  const bySchema = { bronze: [], silver: [], gold: [] };
  for (const t of TABLES.values()) bySchema[t.layer].push(t);
  $("#tree").innerHTML = `<details open><summary>🗂 nexus</summary>${Object.entries(bySchema).map(([s, ts]) =>
    `<details class="schema" ${s === "gold" ? "open" : ""}><summary>${s}</summary>${ts.sort((a, b) => a.name.localeCompare(b.name)).map((t) =>
      `<button data-name="${t.name}">${canRead(t) ? "▦" : '<span class="lock">🔒</span>'} ${t.name.split(".")[2]}<span class="ver">v${t.current}</span></button>`).join("")}</details>`).join("")}</details>`;
  document.querySelectorAll("#tree button").forEach((b) => b.onclick = () => renderTable(TABLES.get(b.dataset.name)));
}

// ---- contract checks notebook, from the compiled contracts --------------------------------
function toDuck(sqlText) {
  return sqlText.replace(/\bnexus_(bronze|silver|gold)\./g, "nexus.$1.")
    .replace(/trunc\((\w+), 'MM'\)/g, "date_trunc('month', $1)");
}

// ---- boot -----------------------------------------------------------------------------------
$("#persona").innerHTML = PERSONAS.map((p) => `<option value="${p.key}">${esc(p.label)}</option>`).join("");
$("#persona").onchange = async (e) => {
  persona = PERSONAS.find((p) => p.key === e.target.value);
  setEngine(`Applying grants and masks for ${persona.label}…`);
  await ready; await refreshViews(); renderTree();
  setEngine(`Running as ${persona.label}`);
};

const ready = boot();
ready.then(async () => {
  renderTree();
  const site = await (await fetch("assets/data/site.json")).json();
  const nb = NOTEBOOKS.find((n) => n.key === "contracts");
  nb.intro = "Each department's YAML contract compiles into SQL checks (src/nexus/contracts.py). These are the same checks, translated to run here against the Delta tables. Zero rows back means the check passes.";
  for (const c of site.contracts) {
    nb.cells.push(md(`<b>${esc(c.department)}</b> · ${esc(c.product)} · owner ${esc(c.owner)} · ${c.passed}/${c.total} passed on the last Spark run`));
    for (const r of c.results.filter((r) => r.kind !== "privacy_columns")) nb.cells.push(sql(`-- ${r.check}\n${toDuck(r.sql)}`));
  }
  NOTEBOOKS.push({ key: "workflow", title: "Workflows", run: site.run });
  $("#tabs").innerHTML = NOTEBOOKS.map((n) => `<button role="tab" data-key="${n.key}">${esc(n.title)}</button>`).join("");
  document.querySelectorAll("#tabs button").forEach((b) => b.onclick = () => {
    const n = NOTEBOOKS.find((x) => x.key === b.dataset.key);
    n.run ? renderWorkflow(n.run) : renderNotebook(n);
  });
  renderNotebook(NOTEBOOKS[0]);
}).catch((e) => setEngine(`Could not start: ${e.message}`));
