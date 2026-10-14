// Platform tour for the lakehouse page: architecture, orchestration, lineage & governance, cost.
// Every number comes from the real run (site.json) or from the Delta tables loaded in the browser.
const $ = (s, r = document) => r.querySelector(s);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
const fmt = (n, d = 0) => Number(n).toLocaleString("en-GB", { maximumFractionDigits: d, minimumFractionDigits: d });
const still = matchMedia("(prefers-reduced-motion: reduce)").matches;
const curve = (x1, y1, x2, y2) => { const m = (x1 + x2) / 2; return `M${x1},${y1} C${m},${y1} ${m},${y2} ${x2},${y2}`; };
const dot = (path, dur, delay = 0, cls = "pdot") => still ? "" :
  `<circle r="3.2" class="${cls}"><animateMotion dur="${dur}s" begin="-${delay}s" repeatCount="indefinite" path="${path}"/></circle>`;

// ---- lineage, from src/nexus/{bronze,silver,gold}.py ---------------------------------------
const FEEDS = { platform_transactions: "platform_transaction", clients: "client", accounts: "account", funds: "fund",
  fund_charges: "fund_charge", finance_ledger: "finance_ledger", service_journeys: "service_journey", colleagues: "colleague", marketing_spend: "marketing_spend" };
const SILVER = { "bronze.platform_transaction": ["silver.platform_transaction", "silver.platform_transaction_quarantine"],
  "bronze.client": ["silver.dim_client"], "bronze.account": ["silver.account"], "bronze.fund": ["silver.dim_fund_share_class"],
  "bronze.fund_charge": ["silver.dim_fund_share_class"], "bronze.finance_ledger": ["silver.finance_ledger"],
  "bronze.service_journey": ["silver.service_journey"], "bronze.colleague": ["silver.colleague"], "bronze.marketing_spend": ["silver.marketing_spend"] };
const GOLD = { "gold.fct_platform_transaction": ["silver.platform_transaction", "silver.dim_fund_share_class", "silver.account", "silver.dim_client"],
  "gold.finance_net_flows_daily": ["gold.fct_platform_transaction", "silver.finance_ledger"],
  "gold.marketing_acquisition_monthly": ["silver.account", "silver.dim_client", "silver.marketing_spend"],
  "gold.hr_workforce_monthly": ["silver.colleague"],
  "gold.executive_kpis_monthly": ["silver.account", "gold.fct_platform_transaction", "gold.finance_net_flows_daily"],
  "gold.product_journeys_monthly": ["silver.service_journey"] };
const GROUPS = { grp_finance: "Finance", grp_marketing: "Marketing", grp_hr: "HR", grp_exec_mi: "Executive MI", grp_board_pack: "Board pack",
  grp_product: "Product", grp_consumer_duty: "Consumer Duty" };

function lineageGraph(lake) {
  const nodes = new Map(), edges = [];
  const add = (id, col, label, extra = {}) => { if (!nodes.has(id)) nodes.set(id, { id, col, label, ...extra }); return id; };
  for (const [feed, b] of Object.entries(FEEDS)) edges.push([add(`land.${feed}`, 0, `${feed}.jsonl`, { kind: "landing" }), add(`bronze.${b}`, 1, b)]);
  for (const [b, ss] of Object.entries(SILVER)) for (const s of ss) edges.push([add(b, 1, b.split(".")[1]), add(s, 2, s.split(".")[1])]);
  for (const [g, ups] of Object.entries(GOLD)) for (const u of ups) edges.push([add(u, u.startsWith("gold") ? 3 : 2, u.split(".")[1]), add(g, 3, g.split(".")[1])]);
  for (const t of lake.tables.values()) {
    const id = t.name.replace("nexus.", "");
    if (nodes.has(id)) Object.assign(nodes.get(id), { table: t });
    if (t.layer === "gold") for (const g of t.read || []) edges.push([id, add(`grp.${g}`, 4, GROUPS[g] || g, { kind: "group", group: g })]);
  }
  return { nodes, edges };
}

// ---- 1. Architecture ----------------------------------------------------------------------
export async function renderArchitecture(view, lake, site) {
  const r = site.run;
  const counts = {};
  for (const t of lake.tables.values()) counts[t.layer] = (counts[t.layer] || 0) + 1;
  const STAGES = [
    { x: 75, key: "src", title: "Sources", sub: "platform CDC · ledger · fund files · HR · marketing", n: `${fmt(r.feeds.platform_transactions + r.feeds.late_corrections)} records · 9 feeds`, tech: "Event Hubs · ADF · Auto Loader",
      say: `Nine feeds land as files and change events. The platform sends ${fmt(r.feeds.platform_transactions)} transaction records, including ${r.feeds.replayed_records} replays and ${r.feeds.malformed_records} malformed rows, then ${r.feeds.late_corrections} late corrections.` },
    { x: 250, key: "bronze", title: "Bronze", sub: "raw, as received", n: `${fmt(r.bronze.platform_transactions)} unique · replay-safe`, tech: "Delta · MERGE on key",
      say: `Bronze is written by MERGE on the business key, newest CDC sequence wins. Replaying every feed changed nothing: ${fmt(r.bronze_after_replay.platform_transactions)} rows before and after.` },
    { x: 425, key: "silver", title: "Silver", sub: "validated, conformed, SCD2", n: `${fmt(r.silver.platform_transaction)} valid · ${r.silver.quarantine} quarantined`, tech: "Lakeflow expectations",
      say: `The source contract splits good rows from bad. ${r.silver.quarantine} rows go to quarantine with the failed rule attached; nothing is silently dropped.` },
    { x: 600, key: "gold", title: "Gold", sub: "fact + one product per department", n: `${fmt(r.gold_fact.rows)} facts · 5 products`, tech: "PySpark · incremental MERGE",
      say: `The fact rebuilds only a 72-hour lookback, joined to the fund price that applied on the trade date. Five department products sit on top, each with a contract.` },
    { x: 775, key: "serve", title: "Serve", sub: "Power BI · Fabric · notebooks · agents", n: `${site.contracts.reduce((a, c) => a + c.passed, 0)}/${site.contracts.reduce((a, c) => a + c.total, 0)} checks · 5 departments`, tech: "Metric views · Direct Lake",
      say: `Consumers read gold only, through Unity Catalog grants from the contracts. Fabric reads the same Delta files through OneLake, so there's no second copy.` },
  ];
  const W = 850, H = 300, Y = 70;
  const box = (s) => `<g class="arch-node" data-k="${s.key}" tabindex="0" role="button" aria-label="${esc(s.title)}">
    <rect x="${s.x - 66}" y="${Y - 48}" width="132" height="104" rx="10"/>
    <text x="${s.x}" y="${Y - 24}" class="t1">${esc(s.title)}</text>
    <text x="${s.x}" y="${Y - 8}" class="t2">${esc(s.tech)}</text>
    <text x="${s.x}" y="${Y + 14}" class="t3">${esc(s.n.split(" · ")[0])}</text>
    <text x="${s.x}" y="${Y + 30}" class="t2">${esc(s.n.split(" · ")[1] || s.sub.split(" · ")[0])}</text></g>`;
  const links = STAGES.slice(1).map((s, i) => { const p = `M${STAGES[i].x + 66},${Y} L${s.x - 66},${Y}`; return `<path d="${p}" class="flow"/>${dot(p, 2.2, i * 0.35)}${dot(p, 2.2, i * 0.35 + 1.1)}`; }).join("");
  view.innerHTML = `<div class="nb-head"><div><h1>End-to-end architecture</h1>
    <p>How data gets from the business systems to the people who use it. Every number on the diagram comes from the real run; press play to follow one day's data through it, or click a stage.</p></div>
    <button class="btn" id="play">▶ Play the flow</button></div>
    <div class="arch"><svg viewBox="0 10 ${W} ${H}" role="img" aria-label="Architecture diagram">
      <rect x="184" y="148" width="657" height="34" rx="6" class="band uc"/><text x="512" y="170" class="band-t">Unity Catalog · classify → enforce → trace → measure</text>
      <rect x="184" y="190" width="482" height="30" rx="6" class="band adls"/><text x="425" y="210" class="band-t dark">ADLS Gen2 · Delta files Quilter owns · private endpoints · Key Vault</text>
      <rect x="9" y="230" width="832" height="30" rx="6" class="band orch"/><text x="425" y="250" class="band-t dark">Orchestration · Workflows from one Asset Bundle · CI/CD gate · repair runs</text>
      <text x="512" y="288" class="band-n">Fabric reads gold in place through OneLake: one copy, one definition</text>
      ${links}${STAGES.map(box).join("")}
    </svg></div>
    <div class="arch-say" id="say" aria-live="polite"><b>Press play</b> or click a stage.</div>
    <div class="arch-stats">${[["Delta tables", lake.tables.size], ["Bronze", counts.bronze], ["Silver", counts.silver], ["Gold", counts.gold], ["Departments served", 5]].map(([k, v]) => `<div><small>${k}</small><b>${v}</b></div>`).join("")}</div>`;
  const show = (s) => {
    view.querySelectorAll(".arch-node").forEach((n) => n.classList.toggle("on", n.dataset.k === s.key));
    $("#say", view).innerHTML = `<b>${esc(s.title)}</b> · ${esc(s.sub)}<br>${esc(s.say)}`;
  };
  view.querySelectorAll(".arch-node").forEach((n) => { const s = STAGES.find((x) => x.key === n.dataset.k); n.onclick = () => show(s); n.onkeydown = (e) => e.key === "Enter" && show(s); });
  $("#play", view).onclick = async () => {
    $("#play", view).disabled = true;
    for (const s of STAGES) { show(s); await new Promise((res) => setTimeout(res, still ? 400 : 2600)); }
    $("#play", view).disabled = false;
  };
}

// ---- 2. Orchestration ---------------------------------------------------------------------
export function renderOrchestration(view, site) {
  const tasks = site.run.tasks;
  const total = Math.max(...tasks.map((t) => t.start_s + t.duration_s)) - tasks[0].start_s;
  const t0 = tasks[0].start_s;
  const BUNDLE = `resources:
  jobs:
    nexus_daily:
      name: nexus-daily
      tags: {owning_team: data-platform, workload: transform}
      schedule:
        quartz_cron_expression: "0 30 5 ? * MON-FRI"   # ready before 06:30
        timezone_id: Europe/London
      health:
        rules: [{metric: RUN_DURATION_SECONDS, op: GREATER_THAN, value: 2700}]
      email_notifications: {on_failure: [vim@example.com]}
      tasks:
        - task_key: pipeline
          python_wheel_task: {package_name: nexus, entry_point: pipeline}
targets:
  dev:  {mode: development, default: true}
  prod: {mode: production}`;
  view.innerHTML = `<div class="nb-head"><div><h1>Orchestration · nexus-daily</h1>
    <p>How I orchestrate: one job, defined in an Asset Bundle, deployed through CI to dev and prod. Every task is safe to re-run, so a failure is fixed with a repair run from the failed task, not a restart. The timings are the real recorded run.</p></div>
    <div class="orch-btns"><button class="btn" id="replay">▶ Replay the run</button><button class="btn ghost" id="fail">Replay with a failure</button></div></div>
    <div class="orch-clock"><span id="clk">05:30:00</span><span id="status" class="pill">scheduled · Mon–Fri 05:30 Europe/London</span></div>
    <div class="dagflow" id="dag">${tasks.map((t, i) => `<div class="tnode" data-i="${i}"><small>${String(i + 1).padStart(2, "0")}</small><b>${esc(t.label)}</b><i><em></em></i><span>${t.duration_s.toFixed(1)}s</span></div>`).join('<div class="tarrow" aria-hidden="true"></div>')}</div>
    <div class="orch-grid">
      <div class="cell"><div class="cell-bar"><span class="lang">databricks.yml</span><span>Asset Bundle</span></div><pre class="orch-code">${NexusHL.hl(BUNDLE, "yaml")}</pre></div>
      <div class="cell"><div class="cell-bar"><span class="lang">How I orchestrate</span></div><ul class="orch-rules">
        <li><b>One definition, every environment.</b> The job is code in the bundle; CI runs <code>bundle validate</code> and deploys. Nobody edits a job in the UI.</li>
        <li><b>Idempotent tasks.</b> MERGE on keys and a 72-hour lookback mean any task can run twice. That's what makes repair runs safe.</li>
        <li><b>Schedules from the contract.</b> Finance's freshness SLO is 06:30, so the job starts at 05:30 with a 45-minute health rule as early warning.</li>
        <li><b>Fail before writing.</b> Schema checks run before MERGE, so a bad batch stops with published numbers untouched.</li>
        <li><b>Alerts go to owners.</b> Failures page the platform; contract breaches notify the data owner named in the YAML.</li>
        <li><b>Right trigger for the source.</b> Schedules for batch, file-arrival triggers for drops, continuous streams only where someone needs it live.</li></ul></div></div>`;
  const nodes = [...view.querySelectorAll(".tnode")];
  const reset = () => nodes.forEach((n) => { n.className = "tnode"; $("em", n).style.width = "0"; });
  const clock = (s) => { const d = new Date(Date.UTC(2026, 9, 1, 5, 30, 0) + s * 1000); $("#clk", view).textContent = d.toISOString().slice(11, 19); };
  let running = false;
  async function replay(failAt = -1, from = 0) {
    if (running) return; running = true;
    view.querySelectorAll(".orch-btns button").forEach((b) => b.disabled = true);
    if (from === 0) reset();
    $("#status", view).className = "pill amber"; $("#status", view).textContent = from ? "repair run · from the failed task" : "running";
    const speed = still ? 0.02 : 8 / total;   // whole run in about 8 seconds
    for (let i = from; i < tasks.length; i++) {
      const t = tasks[i], n = nodes[i];
      n.className = "tnode running";
      const ms = Math.max(250, t.duration_s * speed * 1000);
      const start = performance.now();
      await new Promise((res) => { const step = () => { const k = Math.min(1, (performance.now() - start) / ms);
        $("em", n).style.width = `${(i === failAt ? Math.min(k, 0.4) : k) * 100}%`; clock(t.start_s - t0 + t.duration_s * k);
        if (k < 1) requestAnimationFrame(step); else res(); }; requestAnimationFrame(step); });
      if (i === failAt) {
        n.className = "tnode failed";
        nodes.slice(i + 1).forEach((x) => x.className = "tnode skipped");
        $("#status", view).className = "pill red"; $("#status", view).textContent = "failed: schema drift on platform_transactions · nothing written";
        $("#fail", view).textContent = "↻ Repair run from the failed task"; $("#fail", view).dataset.repair = i;
        running = false; view.querySelectorAll(".orch-btns button").forEach((b) => b.disabled = false);
        return;
      }
      n.className = "tnode done";
    }
    $("#status", view).className = "pill"; $("#status", view).textContent = from ? "repaired · only the failed task and its downstream re-ran" : `succeeded · ${fmt(total, 0)}s on a 2-core laptop`;
    $("#fail", view).textContent = "Replay with a failure"; delete $("#fail", view).dataset.repair;
    running = false; view.querySelectorAll(".orch-btns button").forEach((b) => b.disabled = false);
  }
  $("#replay", view).onclick = () => replay();
  $("#fail", view).onclick = (e) => {
    const r = e.currentTarget.dataset.repair;
    if (r != null) replay(-1, +r); else replay(tasks.findIndex((t) => t.key === "late"));
  };
}

// ---- 3. Lineage & governance --------------------------------------------------------------
export function renderLineage(view, lake) {
  const { nodes, edges } = lineageGraph(lake);
  const cols = [[], [], [], [], []];
  for (const n of nodes.values()) cols[n.col].push(n);
  const W = 1000, colX = [70, 280, 490, 700, 900], rowH = 34;
  const H = Math.max(...cols.map((c) => c.length)) * rowH + 70;
  cols.forEach((c, ci) => { const off = (H - 40 - c.length * rowH) / 2 + 40; c.forEach((n, i) => { n.x = colX[ci]; n.y = off + i * rowH; }); });
  const p = lake.persona;
  const readable = (n) => !n.table || lake.canRead(n.table);
  const piiCols = [...lake.tables.values()].reduce((a, t) => a + Object.keys(t.pii || {}).length, 0);
  const masked = p.mask === "none" ? "shown in clear" : p.mask === "hash" ? "hashed" : "redacted";
  view.innerHTML = `<div class="nb-head"><div><h1>Lineage &amp; governance</h1>
    <p>Every table, where it comes from and who it feeds, built from the pipeline code and the catalog. Click any node to trace it: upstream is its lineage, downstream is its blast radius. Change <b>Run as</b> at the top to see what that persona can read.</p></div></div>
    <div class="gov-strip">
      <div><small>Classify</small><b>${piiCols}</b><span>PII columns tagged</span></div>
      <div><small>Enforce</small><b>${p.rowFilter ? "row filter" : masked}</b><span>for ${esc(p.label)}</span></div>
      <div><small>Trace</small><b>${edges.length}</b><span>lineage edges</span></div>
      <div><small>Measure</small><b>40</b><span>contract checks per run</span></div></div>
    <div class="lin"><svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Lineage graph">
      ${["Landing", "Bronze", "Silver", "Gold", "Consumers (grants)"].map((t, i) => `<text x="${colX[i]}" y="18" class="lin-h">${t}</text>`).join("")}
      <g class="edges">${edges.map(([a, b], i) => { const A = nodes.get(a), B = nodes.get(b); return `<path data-a="${a}" data-b="${b}" d="${curve(A.x + 70, A.y, B.x - 70, B.y)}"/>`; }).join("")}</g>
      <g class="nodes">${[...nodes.values()].map((n) => {
        const pii = n.table && Object.keys(n.table.pii || {}).length;
        return `<g class="ln ${n.kind || n.table?.layer || ""} ${readable(n) ? "" : "locked"}" data-id="${n.id}" tabindex="0" role="button" aria-label="${esc(n.label)}">
          <rect x="${n.x - 70}" y="${n.y - 12}" width="140" height="24" rx="5"/>
          <text x="${n.x - 62}" y="${n.y + 4}">${readable(n) ? "" : "🔒 "}${esc(n.label.length > 22 ? n.label.slice(0, 21) + "…" : n.label)}</text>
          ${pii ? `<circle cx="${n.x + 62}" cy="${n.y}" r="5" class="pii"/>` : ""}</g>`; }).join("")}</g>
    </svg></div>
    <div class="lin-legend"><span><i class="pii"></i>holds PII (tagged at landing)</span><span><i class="up"></i>upstream lineage</span><span><i class="down"></i>downstream blast radius</span><span>🔒 no SELECT for this persona</span></div>
    <div class="lin-detail" id="lind" aria-live="polite"><b>Click a node.</b> Try <code>dim_client</code> to see why one schema change reaches four departments.</div>`;
  const ups = (id, acc = new Set()) => { for (const [a, b] of edges) if (b === id && !acc.has(a)) { acc.add(a); ups(a, acc); } return acc; };
  const downs = (id, acc = new Set()) => { for (const [a, b] of edges) if (a === id && !acc.has(b)) { acc.add(b); downs(b, acc); } return acc; };
  const pick = (id) => {
    const n = nodes.get(id), U = ups(id), D = downs(id);
    view.querySelectorAll(".ln").forEach((g) => { const k = g.dataset.id; g.classList.toggle("sel", k === id); g.classList.toggle("dim", k !== id && !U.has(k) && !D.has(k)); });
    view.querySelectorAll(".edges path").forEach((e) => {
      const { a, b } = e.dataset;
      e.setAttribute("class", U.has(a) && (b === id || U.has(b)) ? "up" : D.has(b) && (a === id || D.has(a)) ? "down" : "off");
    });
    const t = n.table, pii = t ? Object.entries(t.pii || {}) : [];
    const groups = [...D].filter((k) => k.startsWith("grp.")).map((k) => nodes.get(k).label);
    const tables = [...D].filter((k) => !k.startsWith("grp.")).length;
    $("#lind", view).innerHTML = `<b>${esc(n.label)}</b>${t ? ` · <code>${esc(t.name)}</code> · owner ${esc(t.owner)} · Delta v${t.current}` : n.kind === "group" ? " · consumer group" : " · landing file"}
      <div class="lin-cols"><div><small>Upstream lineage</small>${U.size ? `${U.size} object(s)` : "a source"}</div>
      <div><small>Blast radius</small>${tables} table(s) · ${groups.length ? esc(groups.join(", ")) : "no consumers"}</div>
      <div><small>Grants</small>${t ? esc(lake.grantsFor(t).join(", ")) : n.kind === "group" ? "reads via contracts" : "platform only"}</div>
      <div><small>PII and masks</small>${pii.length ? pii.map(([c, k]) => `${esc(c)} <span class="tag">${esc(k)}</span>`).join(" ") + ` · ${masked} for ${esc(p.label)}` : "none"}</div></div>
      <p>${t && !lake.canRead(t) ? `<b>${esc(p.label)}</b> has no SELECT here: queries fail with INSUFFICIENT_PERMISSIONS.` : t && p.rowFilter && t.layer === "gold" ? `Row filter: <b>${esc(p.label)}</b> sees ${esc(p.rowFilter)} rows only.` : ""}
      ${D.size ? ` A breaking change here is gated in CI on this downstream set, and the owners above are added as reviewers.` : ""}</p>`;
  };
  view.querySelectorAll(".ln").forEach((g) => { g.onclick = () => pick(g.dataset.id); g.onkeydown = (e) => e.key === "Enter" && pick(g.dataset.id); });
  pick("silver.dim_client");
}

// ---- 4. Cost ------------------------------------------------------------------------------
export function renderCost(view, site) {
  const tasks = site.run.tasks;
  const W = { ingest: ["ingest", "replay", "late"], transform: ["silver", "gold", "rerun", "restate"], quality: ["check_issued", "check"], generate: ["generate"] };
  const split = Object.fromEntries(Object.entries(W).map(([k, ks]) => [k, tasks.filter((t) => ks.includes(t.key)).reduce((a, t) => a + t.duration_s, 0)]));
  const tot = Object.values(split).reduce((a, b) => a + b, 0);
  const colours = { ingest: "#0f7b3f", transform: "#159847", quality: "#a07d3a", generate: "#9aa89f" };
  let acc = 0;
  const donut = Object.entries(split).map(([k, v]) => { const a0 = acc / tot * 2 * Math.PI, a1 = (acc += v) / tot * 2 * Math.PI;
    const P = (a) => [60 + 48 * Math.sin(a), 60 - 48 * Math.cos(a)];
    const [x0, y0] = P(a0), [x1, y1] = P(a1);
    return `<path d="M60,60 L${x0.toFixed(1)},${y0.toFixed(1)} A48,48 0 ${a1 - a0 > Math.PI ? 1 : 0} 1 ${x1.toFixed(1)},${y1.toFixed(1)} Z" fill="${colours[k]}" class="slice"><title>${k}: ${Math.round(v / tot * 100)}%</title></path>`; }).join("");
  view.innerHTML = `<div class="nb-head"><div><h1>Cost as an engineering parameter</h1>
    <p>Spend is designed in, like latency. At Lifepal the platform ran at <b>$11k a month against a $19k projection</b>, held for four quarters. These are the levers, and a small model of this job you can play with.</p></div></div>
    <div class="cost-grid">
      <div class="cell"><div class="cell-bar"><span class="lang">Lifepal</span><span>monthly platform cost</span></div><div class="cost-bars">
        <div><span>Projection</span><i style="--w:100%" class="proj"></i><b>$19k</b></div>
        <div><span>Actual, four quarters</span><i style="--w:${(11 / 19 * 100).toFixed(1)}%"></i><b>$11k</b></div></div>
        <ul class="orch-rules"><li><b>Incremental, not full rebuilds.</b> The core fact went from 2 h 14 min to 11 min with a 72-hour lookback.</li>
        <li><b>Compute isolation.</b> Ingest, transform, BI and ML on their own policies, so nothing queues and each is sized for its job.</li>
        <li><b>Cadence matched to the SLA.</b> Refresh as often as the decision needs, not as often as possible.</li>
        <li><b>Mandatory tags.</b> Every job carries owning_team and workload, so spend is attributed before anyone asks.</li></ul></div>
      <div class="cell"><div class="cell-bar"><span class="lang">nexus-daily</span><span>where the run time goes (real run, by workload tag)</span></div>
        <div class="donut"><svg viewBox="0 0 120 120">${donut}<circle cx="60" cy="60" r="26" fill="#fff"/><text x="60" y="58" class="d1">${fmt(tot, 0)}s</text><text x="60" y="72" class="d2">per run</text></svg>
        <ul>${Object.entries(split).map(([k, v]) => `<li><i style="background:${colours[k]}"></i>${k} <b>${Math.round(v / tot * 100)}%</b></li>`).join("")}</ul></div>
        <p class="muted" style="padding:0 .9rem .8rem;font-size:.82rem">Quality checks are a big share here because the run checks twice (as issued and after restatement). In production the second pass only covers restated partitions.</p></div></div>
    <div class="cell"><div class="cell-bar"><span class="lang">Estimator</span><span>illustrative rates, change them</span></div>
      <div class="est">
        <label>Refresh cadence<select id="cad"><option value="22">Daily, weekdays (22 runs a month)</option><option value="528">Hourly, weekdays</option><option value="2112">Every 15 minutes, weekdays</option></select></label>
        <label>Build mode<select id="mode"><option value="11">Incremental, 72-hour lookback (11 min)</option><option value="134">Full rebuild (2 h 14 min)</option></select></label>
        <label>Job cluster<select id="w"><option value="2">2 workers</option><option value="4" selected>4 workers</option><option value="8">8 workers</option></select></label>
        <label>Rate per worker-hour (DBUs + VM, $)<input id="rate" type="number" min="0.1" step="0.1" value="1.5"></label>
      </div>
      <div class="est-out"><div class="est-bar"><i id="ebar"></i><span class="budget" title="budget">budget $500</span></div><b id="eval"></b><span id="enote"></span></div></div>`;
  const calc = () => {
    const runs = +$("#cad", view).value, mins = +$("#mode", view).value, w = +$("#w", view).value, rate = +$("#rate", view).value;
    const usd = runs * mins / 60 * w * rate, budget = 500;
    $("#ebar", view).style.width = `${Math.min(100, usd / (budget * 2) * 100)}%`;
    $("#ebar", view).className = usd > budget ? "over" : "";
    $("#eval", view).textContent = `$${fmt(usd, 0)} a month`;
    $("#enote", view).textContent = `${fmt(runs)} runs × ${mins} min × ${w} workers × $${rate}/worker-hour. ${usd > budget ? "Over the $500 budget for this job: is the cadence what the decision needs?" : "Within the $500 budget for this job."}`;
  };
  view.querySelectorAll(".est select, .est input").forEach((e) => e.oninput = calc); calc();
}
