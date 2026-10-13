// NEXUS for Quilter: renders everything from assets/data/site.json, which scripts/build.sh writes
// from a real run of the PySpark pipeline. No numbers on this page are typed by hand.
const REPO = "https://github.com/rfim/nexus-quilter/blob/main/";
const $ = (s, r = document) => r.querySelector(s);
const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
const gbp = (v, d = 0) => "£" + Number(v).toLocaleString("en-GB", { maximumFractionDigits: d, minimumFractionDigits: d });
const short = (v) => Math.abs(v) >= 1e6 ? "£" + (v / 1e6).toFixed(1) + "m" : Math.abs(v) >= 1e3 ? "£" + (v / 1e3).toFixed(0) + "k" : gbp(v);
const mon = (iso) => new Date(iso + "T00:00:00").toLocaleString("en-GB", { month: "short" });

// --- tiny syntax highlighter (comments, strings, keywords, numbers) -------------------------
function highlight(src, lang) {
  const kw = lang === "python"
    ? /\b(?:from|import|def|return|if|for|in|else|elif|and|or|not|None|True|False|with|as|lambda)\b/
    : lang === "sql"
      ? /\b(SELECT|FROM|WHERE|AND|OR|NOT|IN|IS|NULL|AS|ON|JOIN|FULL|LEFT|USING|GROUP|BY|HAVING|WITH|ALTER|TABLE|SET|TAGS|OWNER|TO|GRANT|COMMENT|DATE|INTERVAL|DAYS|count|sum|max|round|abs|coalesce|trunc)\b/
      : null;
  const com = lang === "sql" ? /--[^\n]*/ : /#[^\n]*/;
  const re = new RegExp(`(${com.source})|("(?:[^"\\\\\n]|\\\\.)*"|'[^'\n]*')|(\\b\\d+(?:\\.\\d+)?\\b)` + (kw ? `|(${kw.source})` : ""), "g");
  let out = "", last = 0;
  if (lang === "yaml") {
    return src.split("\n").map((line) => {
      const m = line.match(/^(\s*-?\s*)([\w.]+:)(.*)$/);
      if (!m) return line.trim().startsWith("#") ? `<span class="c">${esc(line)}</span>` : esc(line);
      let rest = m[3], c = "";
      const i = rest.indexOf("  #"); if (i >= 0) { c = rest.slice(i); rest = rest.slice(0, i); }
      return esc(m[1]) + `<span class="k">${esc(m[2])}</span>` + esc(rest).replace(/(".*?")/g, '<span class="s">$1</span>') + (c ? `<span class="c">${esc(c)}</span>` : "");
    }).join("\n");
  }
  src.replace(re, (t, c, s, n, k, off) => {
    out += esc(src.slice(last, off));
    out += `<span class="${c ? "c" : s ? "s" : n ? "n" : "k"}">${esc(t)}</span>`;
    last = off + t.length;
  });
  return out + esc(src.slice(last));
}

// --- charts: plain SVG, one palette ---------------------------------------------------------
const C = { green: "#0f7b3f", mid: "#159847", mint: "#5fc488", dark: "#00301a", amber: "#a07d3a", grey: "#9aa89f", red: "#b3261e" };
function svg(w, h, body) { return `<svg class="chart" viewBox="0 0 ${w} ${h}" role="img">${body}</svg>`; }

function barChart(rows, { label, value, fmt, colour = C.green, h = 220 }) {
  const w = 560, pad = { l: 48, r: 8, t: 14, b: 26 };
  const max = Math.max(...rows.map((r) => Math.abs(value(r))));
  const bw = (w - pad.l - pad.r) / rows.length;
  const y0 = pad.t + (h - pad.t - pad.b);
  let b = `<line class="axis" x1="${pad.l}" x2="${w - pad.r}" y1="${y0}" y2="${y0}"/>`;
  b += `<text x="${pad.l - 6}" y="${pad.t + 4}" text-anchor="end">${fmt(max)}</text>`;
  rows.forEach((r, i) => {
    const v = value(r), bh = (Math.abs(v) / max) * (h - pad.t - pad.b);
    const x = pad.l + i * bw + bw * 0.18;
    b += `<rect x="${x}" y="${y0 - bh}" width="${bw * 0.64}" height="${bh}" rx="2" fill="${v < 0 ? C.amber : colour}"><title>${label(r)}: ${fmt(v)}</title></rect>`;
    b += `<text x="${x + bw * 0.32}" y="${h - 8}" text-anchor="middle">${label(r)}</text>`;
  });
  return svg(w, h, b);
}

function lineChart(series, { x, y, fmt, h = 230 }) {
  const w = 560, pad = { l: 52, r: 120, t: 14, b: 26 };
  const xs = series[0].rows.map(x);
  const all = series.flatMap((s) => s.rows.map(y)).filter((v) => v != null);
  const max = Math.max(...all) * 1.05, min = Math.max(0, Math.min(...all) * 0.8);
  const px = (i) => pad.l + (i / (xs.length - 1)) * (w - pad.l - pad.r);
  const py = (v) => pad.t + (1 - (v - min) / (max - min)) * (h - pad.t - pad.b);
  let b = `<line class="axis" x1="${pad.l}" x2="${w - pad.r}" y1="${py(min)}" y2="${py(min)}"/>`;
  b += `<text x="${pad.l - 6}" y="${pad.t + 4}" text-anchor="end">${fmt(max)}</text><text x="${pad.l - 6}" y="${py(min)}" text-anchor="end">${fmt(min)}</text>`;
  xs.forEach((d, i) => { if (i % 2 === 0) b += `<text x="${px(i)}" y="${h - 8}" text-anchor="middle">${mon(d)}</text>`; });
  series.forEach((s) => {
    const pts = s.rows.map((r, i) => [px(i), y(r)]).filter((p) => p[1] != null).map(([a, v]) => `${a},${py(v)}`);
    b += `<polyline fill="none" stroke="${s.colour}" stroke-width="2.4" points="${pts.join(" ")}"/>`;
    const last = s.rows[s.rows.length - 1];
    b += `<circle cx="${px(s.rows.length - 1)}" cy="${py(y(last))}" r="3.5" fill="${s.colour}"/>`;
    b += `<text class="lbl" x="${px(s.rows.length - 1) + 8}" y="${py(y(last)) + 4}" style="fill:${s.colour}">${s.name} ${fmt(y(last))}</text>`;
  });
  return svg(w, h, b);
}

function hBars(rows, { label, value, fmt, colour = () => C.green, note = () => "" }) {
  const w = 560, rh = 34, pad = { l: 120, r: 140 }, h = rows.length * rh + 10;
  const max = Math.max(...rows.map((r) => value(r) ?? 0));
  let b = "";
  rows.forEach((r, i) => {
    const v = value(r), y = 6 + i * rh;
    b += `<text class="lbl" x="${pad.l - 10}" y="${y + 18}" text-anchor="end">${label(r)}</text>`;
    if (v == null) { b += `<text x="${pad.l}" y="${y + 18}" style="font-style:italic">suppressed (fewer than 5)</text>`; return; }
    const bw = (v / max) * (w - pad.l - pad.r);
    b += `<rect x="${pad.l}" y="${y + 4}" width="${bw}" height="${rh - 12}" rx="3" fill="${colour(r)}"><title>${label(r)}: ${fmt(v)}</title></rect>`;
    b += `<text x="${pad.l + bw + 6}" y="${y + 18}">${fmt(v)}${note(r)}</text>`;
  });
  return svg(w, h, b);
}

// --- departments ----------------------------------------------------------------------------
const DEPTS = [
  { key: "Finance", product: "finance_net_flows_daily", code: "product",
    ask: "Revenue, flows and cost figures they can sign off and reconcile.",
    build: "Point-in-time facts, automated daily reconciliation against the ledger, and restatement evidence before month-end.",
    how: "Finance owns the definition; I own the pipeline. Definitions are agreed in writing before any code. At Lifepal, agreeing what a \"sale\" was took four workshops; the MERGE took an afternoon.",
    quilter: "Fee revenue, AUA and net flows; adviser charges reconciled daily." },
  { key: "Marketing", product: "marketing_acquisition_monthly",
    ask: "Campaign and channel performance quickly enough to act on.",
    build: "Certified acquisition measures (new clients, cost per acquisition) by channel, with self-service on top.",
    how: "Start from the decision the campaign needs. Repeat questions go to self-service. The product carries no client-level columns at all.",
    quilter: "Client acquisition through Quilter advisers, independent advisers and direct, and whether communications are understood." },
  { key: "HR", product: "hr_workforce_monthly",
    ask: "Headcount, attrition and workforce planning.",
    build: "A small aggregate product: headcount and rolling 12-month attrition by function. Records never leave HR.",
    how: "This is the most sensitive data in the business, so access is designed with HR and the DPO before anything is built. The contract suppresses any cell under five.",
    quilter: "Colleague and adviser workforce data, with special-category data handled under GDPR." },
  { key: "Executive", product: "executive_kpis_monthly",
    ask: "One number per question, and confidence that it is right.",
    build: "A short set of certified KPIs, each with its owner, grain and freshness, reconciled to the Finance product so every deck shows the same number.",
    how: "Short written answers, and bad news early with an option attached. Trade-offs are explained in terms of cost, risk and time.",
    quilter: "Board MI and the annual Consumer Duty outcomes assessment." },
  { key: "Product", product: "product_journeys_monthly",
    ask: "What clients experience, fast enough to shape the next release.",
    build: "Service-journey times and completion rates by journey type, from platform events.",
    how: "Embedded with product teams. Event schemas are agreed as contracts, so a new field is reviewed before it reaches a dashboard.",
    quilter: "Consumer Duty consumer support: can clients leave as easily as they join?" },
];

function deptChart(d, P) {
  if (d.key === "Finance") {
    const byMonth = {};
    P.finance_net_flows_monthly.forEach((r) => { byMonth[r.month] = (byMonth[r.month] || 0) + r.net_flows; });
    const rows = Object.entries(byMonth).map(([month, v]) => ({ month, v }));
    return `<h4>Net flows by month, all wrappers</h4>` + barChart(rows, { label: (r) => mon(r.month), value: (r) => r.v, fmt: short });
  }
  if (d.key === "Marketing") {
    const ch = ["Quilter adviser", "Independent adviser", "Direct"], col = [C.green, C.mint, C.amber];
    return `<h4>Cost per new client, by channel</h4>` + lineChart(ch.map((c, i) => ({ name: c.split(" ")[0], colour: col[i],
      rows: P.marketing_acquisition_monthly.filter((r) => r.channel === c) })), { x: (r) => r.month, y: (r) => r.cost_per_acquisition_gbp, fmt: short });
  }
  if (d.key === "HR") {
    const last = P.hr_workforce_monthly.filter((r) => r.month === P.hr_workforce_monthly.at(-1).month).sort((a, b) => (b.attrition_rate_12m ?? 0) - (a.attrition_rate_12m ?? 0));
    const sup = P.hr_workforce_monthly.filter((r) => r.leavers_12m == null).length;
    return `<h4>Rolling 12-month attrition by function, ${mon(last[0].month)} 2026</h4>` +
      hBars(last, { label: (r) => r.function, value: (r) => r.attrition_rate_12m, fmt: (v) => (v * 100).toFixed(1) + "%",
        colour: (r) => r.attrition_rate_12m > 0.12 ? C.amber : C.green, note: (r) => ` · ${r.headcount} people` }) +
      `<p class="note">${sup} of ${P.hr_workforce_monthly.length} monthly cells were suppressed by the contract's minimum cell size.</p>`;
  }
  if (d.key === "Executive") {
    const k = P.executive_kpis_monthly, last = k.at(-1), flows = k.reduce((a, r) => a + r.net_flows_gbp, 0);
    return `<div class="tiles"><div><b>${short(last.aua_gbp)}</b><small>AUA, ${mon(last.month)}</small></div>
      <div><b>${short(flows)}</b><small>Net flows, 12 months</small></div><div><b>${last.active_clients.toLocaleString("en-GB")}</b><small>Active clients</small></div></div>
      <h4>Assets under administration</h4>` + lineChart([{ name: "AUA", colour: C.green, rows: k }], { x: (r) => r.month, y: (r) => r.aua_gbp, fmt: short, h: 190 });
  }
  const j = P.product_journeys_monthly, types = ["transfer_in", "transfer_out", "withdrawal", "switch"];
  const rows = types.map((t) => { const r = j.filter((x) => x.journey_type === t); return { t, v: r.reduce((a, x) => a + x.median_days, 0) / r.length, c: r.reduce((a, x) => a + x.completion_rate, 0) / r.length }; });
  return `<h4>Median days to complete, average of 12 months</h4>` + hBars(rows, { label: (r) => r.t.replace("_", " "), value: (r) => r.v,
    fmt: (v) => v.toFixed(1) + " days", colour: (r) => r.t === "transfer_out" ? C.amber : C.green, note: (r) => ` · ${(r.c * 100).toFixed(0)}% done` }) +
    `<p class="note">Transfers out take longer than transfers in: the gap a Consumer Duty review would ask about.</p>`;
}

function renderDept(d, data) {
  document.querySelectorAll("#dept-tabs button").forEach((b) => b.setAttribute("aria-selected", b.dataset.key === d.key));
  const c = data.contracts.find((x) => x.department === d.key);
  $("#dept-panel").innerHTML = `
    <div><h3>${d.key}</h3><p class="dept-q">“${esc(c.question)}”</p>
      <dl class="dept-rows"><div><dt>What they ask for</dt><dd>${d.ask}</dd></div><div><dt>What I build for them</dt><dd>${d.build}</dd></div>
      <div><dt>How I work with them</dt><dd>${d.how}</dd></div><div class="quilter"><dt>At Quilter</dt><dd>${d.quilter}</dd></div></dl>
      <div class="dept-actions"><button class="link-btn" data-goto="${d.key}">Open the ${d.key} contract →</button>
      <a class="link-btn" href="${REPO}src/nexus/gold.py">See the code ↗</a></div></div>
    <div class="dept-chart">${deptChart(d, data.products)}
      <p class="note">Synthetic data · built by <code>src/nexus/gold.py</code> · contract: ${c.passed}/${c.total} checks passed.</p></div>`;
  $("#dept-panel [data-goto]").onclick = () => { renderContract(d.key, data); $("#contracts").scrollIntoView(); };
}

// --- contracts ------------------------------------------------------------------------------
let view = "yaml", current = "Finance";
function renderContract(dept, data) {
  current = dept;
  const c = data.contracts.find((x) => x.department === dept);
  document.querySelectorAll("#contract-chips button").forEach((b) => b.setAttribute("aria-selected", b.dataset.key === dept));
  const src = { yaml: [c.yaml, "yaml"], expectations: [c.expectations, "python"], governance: [c.governance, "sql"] }[view];
  $("#contract-code").innerHTML = highlight(src[0], src[1]);
  $("#contract-link").href = view === "yaml" ? REPO + c.path : REPO + "src/nexus/contracts.py";
  $("#contract-link").textContent = view === "yaml" ? `See ${c.path} on GitHub ↗` : "See the compiler, src/nexus/contracts.py ↗";
  const fail = c.passed < c.total;
  $("#result-head").innerHTML = `<h3>${esc(c.product)} <span class="score ${fail ? "fail" : ""}">${c.passed}/${c.total} passed</span></h3>
    <p>Owner: ${esc(c.owner)} · checks generated from the contract and run on the last load.</p>`;
  $("#checks").innerHTML = c.results.map((r) => `<li><details class="${r.status}"><summary><span class="st">${r.status}</span>
    <span>${esc(r.check)}</span><span class="kind">${r.kind}</span></summary><pre>${esc(r.sql)}${r.failing_rows ? "\n\n-- failing rows: " + r.failing_rows + "\n" + esc(JSON.stringify(r.sample, null, 1)) : ""}</pre></details></li>`).join("") +
    (fail ? `<li class="callout"><b>The check worked as intended.</b> The generator plants one ledger posting the platform never received. Reconciliation catches the £250 difference on 14 Aug, the product is flagged, and the Finance owner is told before month-end.</li>` : "");
}

// --- platform stages ------------------------------------------------------------------------
function stages(data) {
  const r = data.run, n = (v) => Number(v).toLocaleString("en-GB");
  const totalChecks = data.contracts.reduce((a, c) => a + c.total, 0), passed = data.contracts.reduce((a, c) => a + c.passed, 0);
  return [
    { key: "ingest", t: "Land", stat: n(r.bronze.platform_transactions), d: "unique transactions in bronze, written by MERGE",
      long: `The landing feed held ${n(r.feeds.platform_transactions)} records, including ${r.feeds.replayed_records} replayed offsets and ${r.feeds.corrections} corrections. Deduplication inside the batch plus an ordered MERGE left ${n(r.bronze.platform_transactions)} rows. Replaying every feed changed nothing: ${n(r.bronze_after_replay.platform_transactions)} rows after the replay.` },
    { key: "quality", t: "Prove", stat: n(r.silver.quarantine), d: "malformed rows quarantined, with the rule that failed",
      long: `Rules come from the source contract. ${Object.entries(r.silver.quarantine_reasons).map(([k, v]) => `${v} failed <code>${k}</code>`).join(", ")}. Clean rows continue. Failed rows are kept, with an owner, so they can be inspected and fixed.` },
    { key: "scd2", t: "Historise", stat: "3", d: "funds repriced mid-year, kept as SCD2 versions",
      long: "A changed fund charge must not rewrite last quarter's cost disclosure. Each share-class version keeps its validity window, and facts join to the version in force on the trade date." },
    { key: "fact", t: "Model", stat: n(r.gold_fact.rows_after_rerun), d: "facts after an incremental re-run (unchanged)",
      long: `The fact merges only the last 72 hours (the p99.5 of observed late arrivals), joins each trade to the fund version in force, and is safe to re-run: ${n(r.gold_fact.rows)} rows before, ${n(r.gold_fact.rows_after_rerun)} after.` },
    { key: "compile", t: "Serve", stat: `${passed}/${totalChecks}`, d: "contract checks passed across five departments",
      long: "Each department's product is checked against the contract it signed, on every load. The one failure is the planted ledger break, caught by Finance's reconciliation rule." },
  ];
}

function renderStage(s, data) {
  document.querySelectorAll("#stages button").forEach((b) => b.setAttribute("aria-pressed", b.dataset.key === s.key));
  const code = data.code[s.key];
  $("#stage-detail").innerHTML = `<div><h3>${s.t}</h3><p>${s.long}</p>
    <p><a class="link-btn" href="${REPO}${code.path}">See the code: ${code.path} ↗</a></p></div>
    <div class="code-wrap"><pre class="code" tabindex="0">${highlight(code.source, "python")}</pre></div>`;
}

// --- triage ---------------------------------------------------------------------------------
const REQUESTS = [
  { t: "Board pack AUA is wrong, meeting is tomorrow", q: "do", why: "Critical path for today's executive decision. Fix the number, reconcile it to Finance, and log the root cause as debt to fix properly after First Light." },
  { t: "The daily platform feed failed at 5am", q: "do", why: "A critical-path pipeline. Re-run the interval (replay is safe by design), then write the blameless note the same week." },
  { t: "Rebuild client models on a Data Vault", q: "schedule", why: "Important but not urgent. It goes on the sprint backlog with a design review." },
  { t: "Add freshness SLOs to every gold product", q: "schedule", why: "Governance and quality work that prevents tomorrow's incidents. Schedule it and protect the time." },
  { t: "Marketing wants last month's CPA by channel", q: "delegate", why: "Already a certified measure. Route it to self-service or the governed agent; no hand-written SQL from engineers." },
  { t: "Product asks how many switches happened yesterday", q: "delegate", why: "A routine question the metric layer answers. Show them where it lives once, then it never reaches the team again." },
  { t: "A 40-page monthly report nobody has opened", q: "eliminate", why: "No decision attached. Agree with the owner to retire it, and archive the recurring data dump that feeds it." },
  { t: "A custom extract \"just in case\" for an old campaign", q: "eliminate", why: "Low value and no decision. Decline politely, with a reason and an alternative." },
];
function renderTriage() {
  $("#requests").innerHTML = REQUESTS.map((r, i) => `<li><button aria-pressed="false" data-i="${i}">${esc(r.t)}</button></li>`).join("");
  document.querySelectorAll("#requests button").forEach((b) => b.onclick = () => {
    const r = REQUESTS[b.dataset.i];
    document.querySelectorAll("#requests button").forEach((x) => x.setAttribute("aria-pressed", x === b));
    document.querySelectorAll("#matrix .q").forEach((q) => q.classList.toggle("on", q.dataset.q === r.q));
    $("#matrix .why")?.remove();
    $("#matrix").insertAdjacentHTML("beforeend", `<div class="why">${esc(r.why)}</div>`);
  });
}

// --- boot -----------------------------------------------------------------------------------
fetch("assets/data/site.json").then((r) => r.json()).then((data) => {
  $("#dept-tabs").innerHTML = DEPTS.map((d) => `<button role="tab" data-key="${d.key}">${d.key}</button>`).join("");
  document.querySelectorAll("#dept-tabs button").forEach((b) => b.onclick = () => renderDept(DEPTS.find((d) => d.key === b.dataset.key), data));
  renderDept(DEPTS[0], data);

  const order = DEPTS.map((d) => d.key);
  data.contracts.sort((a, b) => order.indexOf(a.department) - order.indexOf(b.department));
  $("#contract-chips").innerHTML = data.contracts.map((c) => `<button role="tab" data-key="${c.department}">${c.department}<span class="badge ${c.passed < c.total ? "fail" : ""}">${c.passed}/${c.total}</span></button>`).join("");
  document.querySelectorAll("#contract-chips button").forEach((b) => b.onclick = () => renderContract(b.dataset.key, data));
  document.querySelectorAll(".code-tabs button").forEach((b) => b.onclick = () => {
    view = b.dataset.view;
    document.querySelectorAll(".code-tabs button").forEach((x) => x.setAttribute("aria-selected", x === b));
    renderContract(current, data);
  });
  renderContract("Finance", data);

  const st = stages(data);
  $("#stages").innerHTML = st.map((s, i) => `<li><button data-key="${s.key}" aria-pressed="false"><small>Stage ${i + 1}</small><h3>${s.t}</h3><b>${s.stat}</b><p>${s.d}</p></button></li>`).join("");
  document.querySelectorAll("#stages button").forEach((b) => b.onclick = () => renderStage(st.find((s) => s.key === b.dataset.key), data));
  renderStage(st[0], data);
  renderTriage();
});
