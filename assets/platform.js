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
export function renderOrchestration(view, site, lake) {
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
    </div>
    <div class="scen" role="radiogroup" aria-label="Scenario for this run">
      <span>Scenario</span>
      ${[["clean", "Clean run"], ["drift", "Schema drift · fails"], ["small", "Small files"], ["skew", "Data skew"], ["spill", "Spill to disk"]].map(([k, l], i) =>
        `<label><input type="radio" name="scen" value="${k}" ${i ? "" : "checked"}><span>${l}</span></label>`).join("")}
      <div class="orch-btns"><button class="btn" id="replay">▶ Run</button><button class="btn ghost" id="fix" hidden>Apply the fix and re-run</button></div></div>
    <div class="orch-clock"><span id="clk">05:30:00</span><span id="status" class="pill">scheduled · Mon–Fri 05:30 Europe/London</span></div>
    <div class="dagflow" id="dag">${tasks.map((t, i) => `<div class="tnode" data-i="${i}"><small>${String(i + 1).padStart(2, "0")}</small><b>${esc(t.label)}</b><i><em></em></i><span>${t.duration_s.toFixed(1)}s</span></div>`).join('<div class="tarrow" aria-hidden="true"></div>')}</div>
    <div class="stagev" id="stage" aria-live="polite"><div class="sv-h"><b>Spark stage view</b><span id="svt">Pick a scenario and press Run. The stage view shows what the executors are doing inside the running task.</span></div><div class="sv-body" id="svb"></div><div class="sv-m" id="svm"></div></div>
    <div class="cell runlog"><div class="cell-bar"><span class="lang">Run log</span><span>what Spark recorded, and what the team did about it · click an entry</span></div>
      <div class="rl-grid"><ol class="rl-list" id="rl" aria-live="polite"></ol><div class="rl-detail" id="rld"><p class="muted">Measuring the tables…</p></div></div></div>
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
  const log = runLog(view, site, lake);
  const reset = () => nodes.forEach((n, i) => { n.className = "tnode"; $("em", n).style.width = "0"; $("span", n).textContent = `${tasks[i].duration_s.toFixed(1)}s`; });
  const clock = (s) => { const d = new Date(Date.UTC(2026, 9, 1, 5, 30, 0) + s * 1000); $("#clk", view).textContent = d.toISOString().slice(11, 19); };
  const ratio = () => { const m = log.m; return m ? m.entity[0].n / m.entity[1].n : 6; };
  // what each bad thing does to a run, and what the fix changes
  const SCEN = {
    small: { task: "ingest", factor: 4, entry: "small", fixEntry: "smallfix", title: "Small files in bronze",
      bad: () => "Ingest opened 1,240 files of about 190 KB each. Most of the time went on listing, opening and reading footers, not on data.",
      good: () => "With optimizeWrite and autoCompact on, and one OPTIMIZE, ingest read 3 files of about 80 MB each." },
    skew: { task: "gold", factor: () => ratio(), entry: "skew", fixEntry: "skewfix", title: "Data skew in the gold join",
      bad: () => `One shuffle partition held the hot key: its task ran ${ratio().toFixed(1)}× the median while the other executors sat idle.`,
      good: () => `AQE split the hot partition into ${Math.ceil(ratio())} pieces; the slowest task is now close to the median.` },
    spill: { task: "restate", factor: 3, entry: "spill", fixEntry: "spillfix", title: "Spill during the restatement",
      bad: () => "The sort didn't fit in execution memory: Spark wrote 2.4 GB of sorted runs to local disk and merged them back.",
      good: () => "With AQE choosing partition sizes (128 MB) and a narrower restatement window, the sort fits in memory: no spill." },
  };
  const sv = { t: $("#svt", view), b: $("#svb", view), m: $("#svm", view) };
  // ---- stage views: one per scenario, plus an even one for healthy tasks
  function stageView(kind, fixed, label) {
    if (kind === "skew") {
      const r = ratio(), n = fixed ? 7 + Math.ceil(r) : 8;
      sv.t.textContent = `${label} · ${n} shuffle tasks${fixed ? " · AQE skew join on" : " · one hot key"}`;
      sv.b.innerHTML = `<div class="sv-tasks">${Array.from({ length: n }, (_, i) => `<div><small>task ${i}</small><i><em data-i="${i}"></em></i><b></b></div>`).join("")}</div>`;
      const ems = [...sv.b.querySelectorAll("em")];
      return (k) => {
        ems.forEach((e, i) => { const len = fixed ? (i < 7 ? 1 / r * 1.05 : 1 / Math.ceil(r) * 1.1) : (i === 3 ? 1 : 1 / r * (0.9 + (i % 3) * 0.06));
          const done = Math.min(1, k / Math.min(1, len * (fixed ? 1 : 1)));
          e.style.width = `${Math.min(1, k / len) * (fixed ? 100 : i === 3 ? 100 : 100 / r) }%`;
          e.className = !fixed && i === 3 ? "hot" : ""; e.parentNode.nextElementSibling.textContent = done >= 1 && k < 1 && !(fixed) && i !== 3 ? "idle" : ""; });
        sv.m.innerHTML = fixed ? `<span>slowest task <b>${(1.1).toFixed(1)}×</b> median</span><span>executors busy <b>${Math.round(Math.min(1, k) * 100)}%</b></span>`
          : `<span>slowest task <b>${(1 + (r - 1) * k).toFixed(1)}×</b> median</span><span>idle executors <b>${k > 1 / r ? 7 : 0} of 8</b></span>`;
      };
    }
    if (kind === "small") {
      const N = fixed ? 3 : 1240;
      sv.t.textContent = `${label} · ${fixed ? "3 files after OPTIMIZE" : "1,240 files to open"}`;
      sv.b.innerHTML = `<div class="sv-files ${fixed ? "big" : ""}">${Array.from({ length: N }, () => "<i></i>").join("")}</div>`;
      const cells = sv.b.querySelectorAll("i");
      let lit = 0;
      return (k) => {
        const want = Math.floor(k * N); for (; lit < want; lit++) cells[lit].className = "on";
        sv.m.innerHTML = `<span>files opened <b>${fmt(Math.floor(k * N))} / ${fmt(N)}</b></span><span>avg file <b>${fixed ? "80 MB" : "190 KB"}</b></span><span>time on file overhead <b>${fixed ? "4%" : "78%"}</b></span>`;
      };
    }
    if (kind === "spill") {
      sv.t.textContent = `${label} · sort in executor memory`;
      sv.b.innerHTML = `<div class="sv-mem"><div><small>execution memory</small><i><em></em></i></div><div><small>spilled to local disk</small><i class="disk"><em></em></i></div></div>`;
      const [mem, disk] = sv.b.querySelectorAll("em");
      return (k) => {
        const memUse = fixed ? Math.min(0.7, k * 1.4) : Math.min(1, k * 3.3);
        mem.style.width = `${memUse * 100}%`; mem.className = memUse >= 1 ? "hot" : "";
        const spill = fixed ? 0 : Math.max(0, (k - 0.3) / 0.7) * 2.4;
        disk.style.width = `${spill / 2.4 * 100}%`;
        sv.m.innerHTML = `<span>memory <b>${Math.round(memUse * 100)}%</b></span><span>spill <b>${spill.toFixed(1)} GB</b></span>`;
      };
    }
    sv.t.textContent = `${label} · 8 tasks, evenly balanced`;
    sv.b.innerHTML = `<div class="sv-tasks">${Array.from({ length: 8 }, (_, i) => `<div><small>task ${i}</small><i><em></em></i><b></b></div>`).join("")}</div>`;
    const ems = [...sv.b.querySelectorAll("em")];
    sv.m.innerHTML = "";
    return (k) => ems.forEach((e, i) => { e.style.width = `${Math.min(1, k * (1 + (i % 4) * 0.04)) * 100}%`; e.className = ""; });
  }
  let running = false, lastScen = "clean";
  const busy = (on) => { running = on; view.querySelectorAll(".orch-btns button, .scen input").forEach((b) => b.disabled = on); };
  async function replay({ scen = "clean", fixed = false, from = 0 } = {}) {
    if (running) return; busy(true);
    const S = SCEN[scen], failAt = scen === "drift" && !from ? tasks.findIndex((t) => t.key === "late") : -1;
    if (from === 0) { reset(); log.clear(); }
    $("#fix", view).hidden = true;
    $("#status", view).className = "pill amber";
    $("#status", view).textContent = from ? "repair run · from the failed task" : fixed ? `re-run with the fix · ${S.title.toLowerCase()}` : "running";
    const speed = still ? 0.02 : 8 / total;   // a clean run takes about 8 seconds
    let elapsed = from ? tasks[from].start_s - t0 : 0, slow = null;
    for (let i = from; i < tasks.length; i++) {
      const t = tasks[i], n = nodes[i];
      const hit = S && S.task === t.key;
      const f = hit && !fixed ? (typeof S.factor === "function" ? S.factor() : S.factor) : 1;
      const dur = t.duration_s * f;
      n.className = `tnode running${hit && !fixed ? " slow" : ""}`;
      const draw = stageView(hit ? scen : "even", fixed, `${String(i + 1).padStart(2, "0")} · ${t.label}`);
      if (hit && !fixed) { $("#status", view).className = "pill amber"; $("#status", view).textContent = `running · ${S.title.toLowerCase()} · this task is slow`; }
      const ms = Math.max(250, dur * speed * 1000 * (hit && !fixed ? 0.55 : 1));
      const start = performance.now(), e0 = elapsed;
      await new Promise((res) => { const step = () => { const k = Math.min(1, (performance.now() - start) / ms);
        $("em", n).style.width = `${(i === failAt ? Math.min(k, 0.4) : k) * 100}%`; clock(e0 + dur * k); draw(i === failAt ? Math.min(k, 0.4) : k);
        if (k < 1) requestAnimationFrame(step); else res(); }; requestAnimationFrame(step); });
      elapsed += dur;
      if (i === failAt) {
        log.add("drift");
        n.className = "tnode failed";
        nodes.slice(i + 1).forEach((x) => x.className = "tnode skipped");
        sv.t.textContent = `${t.label} · schema check failed before MERGE, nothing written`; sv.m.innerHTML = "";
        $("#status", view).className = "pill red"; $("#status", view).textContent = "failed: schema drift on platform_transactions · nothing written";
        $("#fix", view).hidden = false; $("#fix", view).textContent = "↻ Repair run from the failed task"; $("#fix", view).onclick = () => replay({ scen: "clean", from: i });
        busy(false); $("#replay", view).disabled = false;
        return;
      }
      n.className = `tnode ${hit && !fixed ? "warned" : "done"}`;
      if (hit) { $("span", n).textContent = `${dur.toFixed(1)}s${f > 1 ? ` (${f.toFixed(1)}×)` : " · fixed"}`; slow = { i, dur, f };
        log.add(fixed ? S.fixEntry : S.entry); }
      log.task(t.key);
    }
    const tot = elapsed + (total - tasks.reduce((a, t) => a + t.duration_s, 0));   // plus the gaps between tasks
    if (S && !fixed) {
      $("#status", view).className = "pill amber";
      $("#status", view).textContent = `succeeded, but slow · ${S.title.toLowerCase()} · run took ${fmt(tot, 0)}s instead of ${fmt(total, 0)}s · health alert sent`;
      sv.t.textContent = S.bad(); 
      $("#fix", view).hidden = false; $("#fix", view).textContent = "Apply the fix and re-run"; $("#fix", view).onclick = () => replay({ scen, fixed: true });
    } else {
      $("#status", view).className = "pill";
      $("#status", view).textContent = from ? "repaired · only the failed task and its downstream re-ran" : fixed ? `fixed · run took ${fmt(tot, 0)}s, back to normal` : `succeeded · ${fmt(total, 0)}s on a 2-core laptop`;
      if (fixed) sv.t.textContent = S.good();
    }
    busy(false);
  }
  $("#replay", view).onclick = () => replay({ scen: view.querySelector('input[name="scen"]:checked').value });
  view.querySelectorAll('input[name="scen"]').forEach((r) => r.onchange = () => { $("#fix", view).hidden = true; });
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

// ---- run log: Spark signals, root cause under the hood, the Databricks fix ------------------
async function measure(lake) {
  const m = {};
  const [fund] = await lake.rows("SELECT count(*) AS n FROM nexus.silver.dim_fund_share_class");
  const ft = lake.tables.get("nexus.silver.dim_fund_share_class");
  m.fundRows = fund.n; m.fundBytes = lake.snapshot(ft).files.reduce((a, f) => a + f.size, 0);
  m.entity = await lake.rows("SELECT legal_entity AS k, count(*) AS n FROM nexus.gold.fct_platform_transaction GROUP BY ALL ORDER BY n DESC");
  const [acc] = await lake.rows(`SELECT max(n) AS mx, median(n) AS med, count(*) AS accounts FROM (
    SELECT account_id, count(*) AS n FROM nexus.bronze.platform_transaction GROUP BY ALL)`);
  m.acc = acc;
  m.files = [...lake.tables.values()].map((t) => { const sn = lake.snapshot(t);
    const removed = t.commits.reduce((a, c) => a + c.actions.filter((x) => x.remove).length, 0);
    return { name: t.name, files: sn.files.length, bytes: sn.files.reduce((a, f) => a + f.size, 0), removed, versions: t.current + 1 }; });
  m.removed = m.files.reduce((a, f) => a + f.removed, 0);
  m.tablesWithRemoved = m.files.filter((f) => f.removed).length;
  return m;
}

function bars(items, max, cls = "") {
  return `<div class="rl-bars ${cls}">${items.map(([label, v, note, hot]) => `<div><span>${esc(label)}</span><i style="--w:${Math.max(1.5, v / max * 100)}%" class="${hot ? "hot" : ""}"></i><b>${esc(note ?? fmt(v))}</b></div>`).join("")}</div>`;
}

function runLog(view, site, lake) {
  const r = site.run, fin = site.contracts.find((c) => c.department === "Finance");
  let m = null;
  const ENTRIES = {
    replay: { task: "ingest", level: "INFO", src: "measured here", title: `MERGE collapsed ${r.feeds.replayed_records} replayed records inside the batch`,
      spark: () => `${fmt(r.feeds.platform_transactions)} records arrived; bronze holds ${fmt(r.bronze.platform_transactions)} unique transactions, and the full replay changed 0 rows.`,
      hood: "A MERGE matches source rows to target rows on the key. If the same key appears twice in one batch, Delta refuses to guess which wins and fails the MERGE, so duplicates are removed inside the batch first with a window over the key, ordered by the CDC sequence.",
      fix: ["python", `w = Window.partitionBy("transaction_id").orderBy(F.col("cdc_lsn").desc())
latest = batch.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
target.alias("t").merge(latest.alias("s"), "t.transaction_id = s.transaction_id") \\
    .whenMatchedUpdateAll(condition="s.cdc_lsn > t.cdc_lsn").whenNotMatchedInsertAll().execute()`],
      ba: () => bars([["records in", r.feeds.platform_transactions + r.feeds.late_corrections], ["rows in bronze", r.bronze.platform_transactions], ["rows changed by replay", 0, "0"]], r.feeds.platform_transactions + r.feeds.late_corrections),
      team: ["Detected by the replay test in CI (run twice, compare counts).", "No action needed in production: this is the design working.", "Guard: the test runs on every pull request that touches bronze."] },
    small: { task: "ingest", level: "WARN", src: "production scale", title: "Small files: 1,240 files averaging 190 KB in one bronze table",
      spark: () => `Streaming micro-batches and frequent MERGEs each write new files. At Lifepal, one bronze table reached 1,240 files of about 190 KB, and every query opened all of them. Here, measured live: ${m.files.filter((f) => f.files === 1).length} of ${m.files.length} tables are one file each, because they're compacted.`,
      hood: "Spark plans one task per file split, and every file means a listing call, a footer read and an open on cloud storage. Thousands of tiny files spend more time on that overhead than on reading data, and Delta's statistics for skipping files get less useful.",
      fix: ["sql", `ALTER TABLE nexus.bronze.platform_transaction SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',   -- fewer, larger files at write time
  'delta.autoOptimize.autoCompact'   = 'true');  -- compact small files after writes
OPTIMIZE nexus.bronze.platform_transaction;       -- one-off compaction
-- in Unity Catalog: let predictive optimization schedule OPTIMIZE and VACUUM
ALTER SCHEMA nexus.bronze ENABLE PREDICTIVE OPTIMIZATION;`],
      ba: () => bars([["files before", 1240, "1,240 files", true], ["files after OPTIMIZE", 3, "3 files"], ["scan before", 41, "41 s", true], ["scan after", 6, "6 s"]], 1240) + `<p class="muted">Illustrative, from Lifepal at production scale. Live here: ${m.files.length} tables, ${m.files.reduce((a, f) => a + f.files, 0)} data files in total.</p>`,
      team: ["Detected by a weekly job over the table metadata: alert when average file size falls under 32 MB.", "Triaged by the table owner: is it streaming, frequent MERGE or over-partitioning?", "Fixed with optimizeWrite and autoCompact, then OPTIMIZE once.", "Guard: predictive optimization on the schema, so nobody schedules it by hand."] },
    broadcast: { task: "silver", level: "INFO", src: "measured here", title: () => `BroadcastHashJoin: dim_fund_share_class (${m.fundRows} rows, ${(m.fundBytes / 1024).toFixed(1)} KB) sent to every executor`,
      spark: () => `The fund dimension has ${m.fundRows} rows (${(m.fundBytes / 1024).toFixed(1)} KB), well under the 10 MB auto-broadcast threshold, so Spark copies it to every executor instead of shuffling ${fmt(r.gold_fact.rows)} facts.`,
      hood: "A sort-merge join shuffles both sides by the join key across the network and sorts them. When one side is small, broadcasting it lets every task join locally: no shuffle of the big side at all. AQE can also switch to a broadcast at runtime when it sees a side is small.",
      fix: ["python", `from pyspark.sql.functions import broadcast
fact = txn.join(broadcast(dim_fund), on="isin", how="left")   # explicit, survives stats drift
# spark.sql.autoBroadcastJoinThreshold = 10MB by default; AQE re-plans at runtime`],
      ba: () => bars([["shuffled rows, sort-merge", r.gold_fact.rows + m.fundRows, null, true], ["shuffled rows, broadcast", 0, "0"]], r.gold_fact.rows + m.fundRows),
      team: ["Checked in the query plan during review (EXPLAIN shows BroadcastHashJoin).", "Explicit broadcast hint for dimensions that will always be small.", "Guard: a dimension that grows past the threshold shows up as a new Exchange in the plan diff."] },
    skew: { task: "gold", level: "WARN", src: "measured here", title: () => `Skew: ${esc(m.entity[0].k)} holds ${Math.round(m.entity[0].n / (m.entity.reduce((a, e) => a + e.n, 0)) * 100)}% of the fact; busiest account ${m.acc.mx} rows vs median ${m.acc.med}`,
      spark: () => `Measured on the fact table: ${m.entity.map((e) => `${e.k} ${fmt(e.n)}`).join(", ")} rows. Partitioning by legal_entity would give one task ${(m.entity[0].n / m.entity[1].n).toFixed(1)}× the work of the other. Per account the busiest has ${m.acc.mx} transactions against a median of ${m.acc.med}.`,
      hood: "A shuffle sends every row with the same key to the same task. If one key is hot, that task runs long while the others finish and wait: the stage is as slow as its slowest task, and the Spark UI shows one task far above the median duration.",
      fix: ["sql", `-- AQE splits skewed shuffle partitions at runtime (on by default in Databricks)
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
-- don't partition by a skewed, low-cardinality column; cluster instead
ALTER TABLE nexus.gold.fct_platform_transaction CLUSTER BY (trade_date, account_sk);
-- for a hot key in an aggregation, salt it: group by (key, salt), then by key`],
      ba: () => { const tot = m.entity.reduce((a, e) => a + e.n, 0), half = tot / 2;
        return bars(m.entity.map((e, i) => [`task by ${e.k}`, e.n, `${fmt(e.n)} rows`, i === 0]), m.entity[0].n) + `<p class="muted">After AQE splits the hot partition, each task gets about ${fmt(Math.ceil(half / Math.ceil(m.entity[0].n / half)))}–${fmt(Math.ceil(half))} rows instead of ${fmt(m.entity[0].n)}.</p>`; },
      team: ["Detected from the stage's task-duration distribution (max vs median) in the Spark UI and system tables.", "Triaged: is it a join, an aggregation or the table layout?", "Fixed with AQE skew-join settings and liquid clustering instead of partitionBy(legal_entity).", "Guard: a weekly query on system.lakeflow job runs flags stages whose slowest task exceeds 5× the median."] },
    spill: { task: "restate", level: "WARN", src: "production scale", title: "Spill: 2.4 GB spilled to disk in the restatement sort",
      spark: () => "At Lifepal's volume, the restatement stage sorted more data per task than executor memory held, and Spark wrote 2.4 GB of spill to local disk. The stage took 9 minutes.",
      hood: "When a sort or aggregation doesn't fit in execution memory, Spark writes sorted runs to local disk and merges them later. It doesn't fail, it just gets slow. The usual causes are too few shuffle partitions for the data, skew, or wide rows.",
      fix: ["sql", `SET spark.sql.shuffle.partitions = auto;                 -- let AQE size partitions
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 128MB;
-- and only re-read what changed: restate the affected trade_dates, not the year`],
      ba: () => bars([["spill before", 2.4, "2.4 GB", true], ["spill after", 0.01, "0"], ["stage before", 9, "9 min", true], ["stage after", 3, "3 min"]], 9) + '<p class="muted">Illustrative, from Lifepal at production scale.</p>',
      team: ["Detected from spill metrics on the stage (Spark UI, system tables).", "Triaged: partition count, skew or memory?", "Fixed with AQE auto partitions and a narrower restatement window; Photon on the job cluster.", "Guard: health rule on run duration (45 min) catches regressions before the SLO."] },
    late: { task: "late", level: "INFO", src: "measured here", title: `Late batch: ${r.feeds.late_corrections} corrections applied by MERGE, gold restated`,
      spark: () => `${r.feeds.late_corrections} corrections arrived after the first publish. MERGE updated the existing rows (newest CDC sequence wins), and silver and gold re-ran for the affected dates.`,
      hood: "Delta writes a new table version for the MERGE; the old version stays readable, which is how Finance can see what was issued and what changed.",
      fix: ["sql", `-- what was issued vs now, straight from the Delta history
SELECT n.trade_date, n.wrapper_code, i.net_flows_gbp AS issued, n.net_flows_gbp AS now
FROM nexus.gold.finance_net_flows_daily n
JOIN nexus.gold.finance_net_flows_daily VERSION AS OF 0 i USING (trade_date, wrapper_code, legal_entity)
WHERE n.net_flows_gbp <> i.net_flows_gbp`],
      ba: () => bars([["corrections", r.feeds.late_corrections], ["new rows", 0, "0"]], r.feeds.late_corrections),
      team: ["Expected, not an incident: the design restates by MERGE.", "Finance is told which days changed, with the time-travel diff attached.", "Guard: the lookback (72 h) is the p99.5 of observed lateness, re-measured quarterly."] },
    recon: { task: "check_issued", level: "ERROR", src: "measured here", title: `Contract check failed: ${fin.product} reconcile to finance_ledger (1 row, £250)`,
      spark: () => "One day and wrapper, 14 Aug ISA, differs from the ledger by £250. Quarantine was empty that day and bronze equals gold, so the pipeline is clean.",
      hood: "Not a Spark problem: the check compares two independent systems. It's in the log because a failing contract is an operational event with an owner.",
      fix: ["sql", `SELECT trade_date, wrapper_code, platform_gbp, ledger_gbp, ledger_gbp - platform_gbp AS diff
FROM reconciliation WHERE abs(diff) > 0.01   -- compiled from the contract's tolerance_gbp`],
      ba: () => bars([["platform", 1522.70, "£1,522.70"], ["ledger", 1772.70, "£1,772.70", true]], 1772.70),
      team: ["Detected by the contract check; the Finance owner is notified from the YAML.", "Triaged with three queries (Chapter 5, Keep it running).", "Fixed at the source; the check stays red until the posting is resent.", "Guard: a source-to-ledger completeness check at bronze."] },
    vacuum: { task: "check", level: "INFO", src: "measured here", title: () => `VACUUM candidates: ${m.removed} superseded files across ${m.tablesWithRemoved} tables`,
      spark: () => `Every rewrite leaves the old files in place for time travel. Across the lakehouse, ${m.removed} files have been logically removed but are still on storage.`,
      hood: "Delta never deletes data in place: a remove action in the log hides a file from new versions, but the file stays until VACUUM, so older versions still work. Retention is the trade-off between storage cost and how far back you can travel.",
      fix: ["sql", `ALTER TABLE nexus.gold.finance_net_flows_daily SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 90 days',   -- audit window
  'delta.logRetentionDuration'         = 'interval 90 days');
VACUUM nexus.gold.finance_net_flows_daily;                      -- or predictive optimization`],
      ba: () => bars(m.files.filter((f) => f.removed).slice(0, 6).map((f) => [f.name.replace("nexus.", ""), f.removed, `${f.removed} file(s)`]), Math.max(...m.files.map((f) => f.removed), 1)),
      team: ["Retention agreed with the DPO and Finance (90 days for certified products).", "VACUUM run by predictive optimization.", "Guard: a restatement older than the window is reproduced from vault satellites instead."] },
    drift: { task: "late", level: "ERROR", src: "scenario", title: "Schema drift: amount_gbp missing, amount unexpected · late batch rejected before MERGE",
      spark: () => "The schema check compared the batch with the source contract and stopped the task before anything was written. Published numbers stayed on the last good version.",
      hood: "Without the check, Delta would either fail the MERGE on a missing column or, with schema evolution switched on, add a new column and leave amount_gbp null, which silently zeroes net flows.",
      fix: ["yaml", `# contracts/sources/sources.yml: the agreed interface
columns:
  - {name: amount_gbp, type: "decimal(18,2)", required: true}
schema_evolution: reject      # a rename is a contract change, not a hotfix`],
      ba: () => bars([["rows written", 0, "0"], ["published numbers changed", 0, "0"]], 1),
      team: ["Detected by the schema check; the platform owner is paged.", "Triaged: a rename upstream, agreed as a contract change the same day.", "Fixed: contract PR merged, then a repair run from this task.", "Guard: the source team's CI validates against our contract before they deploy."] },
  };
  const fixed = (k, title, ba) => ({ ...ENTRIES[k], level: "INFO", src: "re-run with the fix", title, ba });
  ENTRIES.smallfix = fixed("small", "Fixed: optimizeWrite + autoCompact + OPTIMIZE · 3 files, ingest back to its usual time",
    () => bars([["files", 3, "3 files"], ["ingest time", 1, "usual"]], 3));
  ENTRIES.skewfix = fixed("skew", () => `Fixed: AQE skew join split the hot partition · slowest task 1.1× median`,
    () => bars([["slowest task / median, before", m.entity[0].n / m.entity[1].n, `${(m.entity[0].n / m.entity[1].n).toFixed(1)}×`, true], ["after", 1.1, "1.1×"]], m.entity[0].n / m.entity[1].n));
  ENTRIES.spillfix = fixed("spill", "Fixed: AQE partition sizing · spill 0, restatement back to its usual time",
    () => bars([["spill before", 2.4, "2.4 GB", true], ["spill after", 0.01, "0"]], 2.4));
  const ORDER = ["replay", "small", "broadcast", "skew", "spill", "late", "recon", "vacuum"];
  const val = (x) => typeof x === "function" ? x() : x;
  const shown = [];
  const t0 = site.run.tasks[0].start_s;
  const stamp = (key) => { const t = site.run.tasks.find((x) => x.key === ENTRIES[key].task) || site.run.tasks[0];
    const d = new Date(Date.UTC(2026, 9, 1, 5, 30, 0) + (t.start_s - t0 + t.duration_s * 0.8) * 1000); return d.toISOString().slice(11, 19); };
  const draw = () => {
    if (!m) return;
    $("#rl", view).innerHTML = [...shown].sort((a, b) => stamp(a).localeCompare(stamp(b))).map((k) => { const e = ENTRIES[k];
      return `<li><button data-k="${k}" class="lvl-${e.level.toLowerCase()}"><span class="ts">${stamp(k)}</span><span class="lv">${e.level}</span><span class="tk">${esc(e.task)}</span><span class="msg">${esc(val(e.title))}</span><span class="src">${e.src}</span></button></li>`; }).join("");
    view.querySelectorAll("#rl button").forEach((b) => b.onclick = () => open(b.dataset.k));
  };
  const open = (k) => {
    const e = ENTRIES[k];
    view.querySelectorAll("#rl button").forEach((b) => b.classList.toggle("on", b.dataset.k === k));
    $("#rld", view).innerHTML = `<div class="rl-h"><span class="pill ${e.level === "ERROR" ? "red" : e.level === "WARN" ? "amber" : ""}">${e.level}</span> <b>${esc(val(e.title))}</b> <span class="src-tag">${e.src}</span></div>
      <h4>What Spark recorded</h4><p>${esc(e.spark())}</p>
      <h4>Under the hood</h4><p>${esc(e.hood)}</p>
      <h4>The fix, Databricks best practice</h4><pre class="orch-code">${NexusHL.hl(e.fix[1], e.fix[0])}</pre>
      <h4>Before → after</h4>${e.ba()}
      <h4>How the team handles it</h4><ol class="rl-steps">${e.team.map((t) => `<li>${esc(t)}</li>`).join("")}</ol>`;
  };
  measure(lake).then((res) => { m = res; shown.push(...ORDER); draw(); open("skew"); })
    .catch((err) => { $("#rld", view).innerHTML = `<div class="err">${esc(err.message)}</div>`; });
  return {
    get m() { return m; },
    clear() { shown.length = 0; draw(); $("#rld", view).innerHTML = '<p class="muted">The run is in progress. Entries appear here as each task finishes.</p>'; },
    task(key) { for (const k of ORDER) if (ENTRIES[k].task === key && !shown.includes(k)) shown.push(k); draw(); },
    add(k) { if (!shown.includes(k)) shown.push(k); draw(); if (m) open(k); },
  };
}
