// NEXUS Lakehouse: the real Delta tables from the PySpark run, replayed in the browser.
//
// Delta part: for each table we fetch its _delta_log commits (newline-delimited JSON actions)
// and replay add/remove actions to get the exact set of Parquet files at any version. That gives
// DESCRIBE HISTORY, DESCRIBE DETAIL and VERSION AS OF / TIMESTAMP AS OF straight from the log.
// Query part: parquet-wasm (Rust arrow-rs) decodes each data file a version points to, and
// DuckDB-WASM runs the SQL over the Arrow data. No DuckDB extension is downloaded, so it also
// works on networks that block extensions.duckdb.org.
// Unity Catalog part: three-level names (nexus.<schema>.<table>), grants, column masks and row
// filters, applied according to the persona you "run as". It reproduces the behaviour in the
// browser; it is not Databricks itself.
import { openLakehouse, PERSONAS } from "./engine.js";
import { renderArchitecture, renderOrchestration, renderLineage, renderCost } from "./platform.js";

const $ = (s, r = document) => r.querySelector(s);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
let lake;
const runSQL = (s) => lake.runSQL(s);
const canRead = (t) => lake.canRead(t);
const snapshot = (t, v) => lake.snapshot(t, v);
const setEngine = (s) => { $("#engine").textContent = s; };

function renderResult(el, res, err) {
  if (err) { el.innerHTML = `<div class="err">${esc(err.message || err)}</div>`; return; }
  const head = `<tr>${res.columns.map((c) => `<th>${esc(c)}</th>`).join("")}</tr>`;
  const body = res.rows.map((r) => `<tr>${r.map((v) => v == null ? '<td class="null">null</td>'
    : `<td class="${typeof v === "number" ? "num" : ""}">${esc(typeof v === "number" ? (Number.isInteger(v) ? v : +v.toFixed(4)) : v)}</td>`).join("")}</tr>`).join("");
  const n = res.total ?? res.rows.length;
  el.innerHTML = `<div class="meta">${n.toLocaleString("en-GB")} row${n === 1 ? "" : "s"}${n > res.rows.length ? ` · showing ${res.rows.length}` : ""} · ${res.ms} ms · run as ${esc(lake.persona.label)}</div><table>${head}${body}</table>`;
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

let current = "arch";
let showView = () => {};

function renderNotebook(nb) {
  current = nb.key;
  document.querySelectorAll("#tabs button").forEach((b) => b.setAttribute("aria-selected", b.dataset.key === nb.key));
  const view = $("#view");
  view.innerHTML = `<div class="nb-head"><div><h1>${esc(nb.title)}</h1><p>${nb.intro}</p></div><button class="btn" id="runall">Run all</button></div><div id="cells"></div>`;
  const wrap = $("#cells");
  nb.cells.forEach((c, i) => {
    if (c.md) { wrap.insertAdjacentHTML("beforeend", `<div class="cell md"><p>${c.md}</p></div>`); return; }
    const el = document.createElement("div");
    el.className = "cell";
    el.innerHTML = `<div class="cell-bar"><span class="lang">SQL</span><span>Cmd ${i + 1}</span><button class="btn">Run</button></div>
      <div class="ed"><pre class="ed-hl hl-light" aria-hidden="true"></pre><textarea spellcheck="false" aria-label="SQL cell ${i + 1}" rows="${Math.min(14, c.sql.split("\n").length + 1)}">${esc(c.sql)}</textarea></div><div class="out" hidden></div>`;
    const ta = $("textarea", el), out = $(".out", el), pre = $(".ed-hl", el);
    // coloured SQL behind a transparent textarea, kept in step as you type and scroll
    const paint = () => { pre.innerHTML = NexusHL.hl(ta.value, "sql") + "\n "; pre.scrollTop = ta.scrollTop; pre.scrollLeft = ta.scrollLeft; };
    ta.addEventListener("input", paint); ta.addEventListener("scroll", paint); paint();
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

async function renderTable(t) {
  current = "table";
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
    <div class="cell"><div class="cell-bar"><span class="lang">Sample data</span><span>run as ${esc(lake.persona.label)}</span></div><div class="out" id="sample"></div></div></div>`;
  await ready;
  renderResult($("#cols"), await runSQL(`DESCRIBE ${t.name}`));
  renderResult($("#hist"), await runSQL(`DESCRIBE HISTORY ${t.name}`));
  try { renderResult($("#sample"), await runSQL(`SELECT * FROM ${t.name} LIMIT 20`)); } catch (e) { renderResult($("#sample"), null, e); }
}

function renderTree() {
  const bySchema = { bronze: [], silver: [], gold: [] };
  for (const t of lake.tables.values()) bySchema[t.layer].push(t);
  $("#tree").innerHTML = `<details open><summary>🗂 nexus</summary>${Object.entries(bySchema).map(([s, ts]) =>
    `<details class="schema" ${s === "gold" ? "open" : ""}><summary>${s}</summary>${ts.sort((a, b) => a.name.localeCompare(b.name)).map((t) =>
      `<button data-name="${t.name}">${canRead(t) ? "▦" : '<span class="lock">🔒</span>'} ${t.name.split(".")[2]}<span class="ver">v${t.current}</span></button>`).join("")}</details>`).join("")}</details>`;
  document.querySelectorAll("#tree button").forEach((b) => b.onclick = () => renderTable(lake.tables.get(b.dataset.name)));
}

// ---- contract checks notebook, from the compiled contracts --------------------------------
function toDuck(sqlText) {
  return sqlText.replace(/\bnexus_(bronze|silver|gold)\./g, "nexus.$1.")
    .replace(/trunc\((\w+), 'MM'\)/g, "date_trunc('month', $1)");
}

// ---- boot -----------------------------------------------------------------------------------
$("#persona").innerHTML = PERSONAS.map((p) => `<option value="${p.key}">${esc(p.label)}</option>`).join("");
$("#persona").onchange = async (e) => {
  await ready;
  const p = PERSONAS.find((x) => x.key === e.target.value);
  setEngine(`Applying grants and masks for ${p.label}…`);
  await lake.setPersona(p.key); renderTree();
  if (current === "lineage") showView("lineage");
  setEngine(`Running as ${p.label}`);
};

const ready = openLakehouse({ status: setEngine }).then((l) => { lake = l; });
ready.then(async () => {
  renderTree();
  const site = await (await fetch("assets/data/site.json")).json();
  const nb = NOTEBOOKS.find((n) => n.key === "contracts");
  nb.intro = "Each department's YAML contract compiles into SQL checks (src/nexus/contracts.py). These are the same checks, translated to run here against the Delta tables. Zero rows back means the check passes.";
  for (const c of site.contracts) {
    nb.cells.push(md(`<b>${esc(c.department)}</b> · ${esc(c.product)} · owner ${esc(c.owner)} · ${c.passed}/${c.total} passed on the last Spark run`));
    for (const r of c.results.filter((r) => r.kind !== "privacy_columns")) nb.cells.push(sql(`-- ${r.check}\n${toDuck(r.sql)}`));
  }
  const TOUR = [
    { key: "arch", title: "Architecture", go: () => renderArchitecture($("#view"), lake, site) },
    { key: "orch", title: "Orchestration", go: () => renderOrchestration($("#view"), site) },
    { key: "lineage", title: "Lineage & governance", go: () => renderLineage($("#view"), lake) },
    { key: "cost", title: "Cost", go: () => renderCost($("#view"), site) },
  ];
  $("#tabs").innerHTML = `<span class="tab-group">Platform</span>${TOUR.map((n) => `<button role="tab" data-key="${n.key}">${esc(n.title)}</button>`).join("")}
    <span class="tab-group">Notebooks</span>${NOTEBOOKS.map((n) => `<button role="tab" data-key="${n.key}">${esc(n.title)}</button>`).join("")}`;
  const open = (key) => {
    current = key;
    document.querySelectorAll("#tabs button").forEach((b) => b.setAttribute("aria-selected", b.dataset.key === key));
    const t = TOUR.find((x) => x.key === key);
    if (t) t.go(); else renderNotebook(NOTEBOOKS.find((x) => x.key === key));
  };
  showView = open;
  document.querySelectorAll("#tabs button").forEach((b) => b.onclick = () => open(b.dataset.key));
  open(TOUR.some((t) => t.key === location.hash.slice(1)) ? location.hash.slice(1) : "arch");
}).catch((e) => setEngine(`Could not start: ${e.message}`));
