// Chapter 8 · Let them ask: the semantic layer as self-service, governed and cited.
import { $, esc, fmt, header, lakehouse, table } from "./jdkit.js";

header("ask.html");

const OWNERS = { finance: "finance-reporting-lead@", marketing: "head-of-marketing@", hr: "people-analytics-lead@", exec: "chief-data-officer@", product: "head-of-platform-product@" };
const VIEWS = {
  finance: { name: "nexus.semantic.finance_flows", source: "nexus.gold.finance_net_flows_daily", time: "trade_date",
    dims: { wrapper: "wrapper_code", "legal entity": "legal_entity", month: "date_trunc('month', trade_date)" },
    measures: { net_flows: ["sum(net_flows_gbp)", "Net flows", "£", "Contributions minus withdrawals and fees; switches and income excluded."],
      gross_inflows: ["sum(gross_inflows_gbp)", "Gross inflows", "£", "Contributions in."], gross_outflows: ["sum(gross_outflows_gbp)", "Gross outflows", "£", "Withdrawals and fees out."] } },
  marketing: { name: "nexus.semantic.acquisition", source: "nexus.gold.marketing_acquisition_monthly", time: "month",
    dims: { channel: "channel", month: "month" },
    measures: { cpa: ["sum(spend_gbp) / nullif(sum(new_clients), 0)", "Cost per acquisition", "£", "Marketing spend divided by new clients, by first account opened."],
      new_clients: ["sum(new_clients)", "New clients", "", "Clients whose first account opened in the period."], spend: ["sum(spend_gbp)", "Marketing spend", "£", "Spend by channel."] } },
  hr: { name: "nexus.semantic.workforce", source: "nexus.gold.hr_workforce_monthly", time: "month", latest: true,
    dims: { function: "function", month: "month" },
    measures: { attrition: ["sum(leavers_12m) / nullif(sum(headcount), 0)", "Attrition (rolling 12 months)", "%", "Leavers in the last 12 months over headcount; cells under 5 suppressed."],
      headcount: ["sum(headcount)", "Headcount", "", "Colleagues at month end."] } },
  exec: { name: "nexus.semantic.executive", source: "nexus.gold.executive_kpis_monthly", time: "month",
    dims: { month: "month" },
    measures: { aua: ["sum(aua_gbp)", "Assets under administration", "£", "Month-end AUA across the platform."], active_clients: ["sum(active_clients)", "Active clients", "", "Clients with a funded account at month end."] }, stock: ["aua", "active_clients"] },
  product: { name: "nexus.semantic.journeys", source: "nexus.gold.product_journeys_monthly", time: "month",
    dims: { journey: "journey_type", month: "month" },
    measures: { journey_days: ["round(avg(median_days), 1)", "Median days to complete", " days", "Median calendar days from request to completion."],
      completion_rate: ["sum(completed) / nullif(sum(requested), 0)", "Completion rate", "%", "Completed over requested journeys."] } },
};
const yamlFor = (k) => { const v = VIEWS[k];
  return `version: 0.1
name:   ${v.name}
source: ${v.source}
owner:  ${OWNERS[k]}example.com
dimensions:
${Object.entries(v.dims).map(([n, e]) => `  - name: ${n.replace(" ", "_")}\n    expr: ${e}`).join("\n")}
measures:
${Object.entries(v.measures).map(([n, [e, , , d]]) => `  - name: ${n}\n    expr: ${e}\n    comment: "${d}"`).join("\n")}`; };

// question → {view, measure, dims, window} using words that exist in the semantic layer
const MATCH = [
  [/net flow|flows?\b/, "finance", "net_flows"], [/inflow|came in|money in/, "finance", "gross_inflows"], [/outflow|went out|money out/, "finance", "gross_outflows"],
  [/\bcpa\b|cost per acq|acquisition cost/, "marketing", "cpa"], [/new client|acquired|acquisition/, "marketing", "new_clients"], [/spend|budget/, "marketing", "spend"],
  [/attrition|leaver|turnover/, "hr", "attrition"], [/headcount|how many (people|staff|colleagues)/, "hr", "headcount"],
  [/\baua\b|assets under/, "exec", "aua"], [/active client/, "exec", "active_clients"],
  [/completion rate|completed/, "product", "completion_rate"], [/journey|transfer|withdraw|how long|take to/, "product", "journey_days"],
];
const MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
let LATEST = "2026-09-01";

function understand(text) {
  const q = text.toLowerCase();
  if (/should i|advice|recommend (me|a fund)|move my|invest in/.test(q)) return { refuse: "advice" };
  if (/\bnames?\b|email|national insurance|\bni\b|list (the |all )?(clients|vulnerable|colleagues)|who (are|is) (the )?vulnerable|which clients/.test(q)) return { refuse: "pii" };
  const m = MATCH.find(([re]) => re.test(q));
  if (!m) return { refuse: "undefined", term: ((q.match(/(?:what'?s|what is|what are|show me|how is)\s+([a-z ]{3,30}?)(\?|$| by| last| this| in)/) || [])[1] || "").replace(/^(our|the|my)\s+/, "") };
  const [, view, measure] = m, V = VIEWS[view];
  const dims = Object.keys(V.dims).filter((d) => new RegExp(`by ${d.split(" ")[0]}|per ${d.split(" ")[0]}|each ${d.split(" ")[0]}`).test(q) || (d === "month" && /trend|over time|by month|monthly|each month/.test(q)));
  if (view === "product" && !dims.includes("journey") && !/transfer|withdraw|switch/.test(q)) dims.unshift("journey");
  if (view === "hr" && measure === "attrition" && /which|highest|lowest/.test(q) && !dims.includes("function")) dims.push("function");
  const filters = [];
  const wr = q.match(/\b(jisa|isa|pension|gia|bond)\b/); if (wr && view === "finance") filters.push([V.dims.wrapper, wr[1].toUpperCase()]);
  const le = q.match(/\b(affluent|hnw)\b/); if (le && view === "finance") filters.push([V.dims["legal entity"], le[1]]);
  const ch = q.match(/(quilter adviser|independent adviser|direct)/); if (ch && view === "marketing") filters.push(["channel", ch[1].replace(/\b\w/g, (c) => c.toUpperCase())]);
  const jt = /transfer/.test(q) ? (/\bout\b/.test(q) ? "transfer_out" : /\bin\b/.test(q) ? "transfer_in" : null) : /withdraw/.test(q) ? "withdrawal" : /switch/.test(q) ? "switch" : null;
  if (jt && view === "product") filters.push(["journey_type", jt]);
  let window;
  const mon = MONTHS.findIndex((x) => q.includes(x));
  if (/last month|latest|this month|right now|currently|today|\bnow\b/.test(q) || (V.stock?.includes(measure) && !dims.includes("month")) || (V.latest && !dims.includes("month"))) window = { from: LATEST, label: "the latest month" };
  else if (mon >= 0) { const y = mon >= 9 ? 2025 : 2026; const f = `${y}-${String(mon + 1).padStart(2, "0")}-01`; window = { from: f, label: `${MONTHS[mon][0].toUpperCase()}${MONTHS[mon].slice(1)} ${y}` }; }
  else if (/this year|2026|year to date|ytd/.test(q)) window = { from: "2026-01-01", to: "2026-12-31", label: "2026 so far" };
  else window = { label: "the last 12 months" };
  return { view, measure, dims, filters, window };
}

function compile(u) {
  const V = VIEWS[u.view], [expr, label, unit] = V.measures[u.measure];
  const sel = u.dims.map((d) => `${V.dims[d]} AS ${d.replace(" ", "_")}`);
  const where = [];
  if (u.window.from && !u.window.to) where.push(`${V.time} >= DATE '${u.window.from}' AND ${V.time} < DATE '${u.window.from}' + INTERVAL 1 MONTH`);
  if (u.window.to) where.push(`${V.time} BETWEEN DATE '${u.window.from}' AND DATE '${u.window.to}'`);
  for (const [c, v] of u.filters) where.push(`${c} = '${v}'`);
  const sql = `SELECT ${[...sel, `${unit === "%" ? `round(${expr}, 4)` : unit === "£" ? `round(${expr}, 2)` : expr} AS ${u.measure}`].join(",\n       ")}
FROM ${V.source}${where.length ? `\nWHERE ${where.join("\n  AND ")}` : ""}${u.dims.length ? "\nGROUP BY ALL\nORDER BY " + (u.dims.includes("month") ? "month" : `${u.measure} DESC`) : ""}`;
  const mv = `SELECT ${[...u.dims.map((d) => d.replace(" ", "_")), `MEASURE(${u.measure})`].join(", ")}
FROM ${V.name}${u.window.from ? `\nWHERE ${u.window.label}` : ""}${u.filters.length ? (u.window.from ? " AND " : "\nWHERE ") + u.filters.map(([c, v]) => `${c} = '${v}'`).join(" AND ") : ""}${u.dims.length ? `\nGROUP BY ${u.dims.map((d) => d.replace(" ", "_")).join(", ")}` : ""}`;
  return { sql, mv, label, unit };
}

const show = (v, unit) => v == null ? "n/a" : unit === "£" ? `£${fmt(v, Math.abs(v) >= 1000 ? 0 : 2)}` : unit === "%" ? `${(v * 100).toFixed(1)}%` : `${fmt(v, unit === " days" ? 1 : 0)}${unit}`;

let lake, who = "exec";
const say = (html, cls = "bot") => { const t = $("#thread"); t.insertAdjacentHTML("beforeend", `<div class="${cls}">${html}</div>`); t.scrollTop = t.scrollHeight; return t.lastElementChild; };

async function ask(text) {
  if (!text.trim()) return;
  say(esc(text), "me");
  const u = understand(text);
  const P = lake.PERSONAS.find((p) => p.key === who);
  if (u.refuse === "advice") return say(`<b>I can't give financial advice.</b> I answer questions about the business's own data. For a client's decision, the right route is their adviser.`, "bot refuse");
  if (u.refuse === "pii") return say(`<b>That's personal data, so it isn't in the semantic layer.</b> I only answer from governed aggregates. Outcomes for vulnerable clients are available in aggregate (Chapter 6, <a href="duty.html">Protect clients</a>); record-level access goes through the data owner and the DPO.`, "bot refuse");
  if (u.refuse === "undefined") return say(`<b>${u.term ? `“${esc(u.term.trim())}” isn't` : "That isn't"} a certified metric yet, so I won't guess a definition.</b> Two people asking the same question should get the same number. I've logged it as a request: it needs an owner, a definition in a contract, and a metric view. Try net flows, AUA, active clients, cost per acquisition, attrition or journey times.`, "bot refuse");
  const V = VIEWS[u.view], c = compile(u), t = lake.tables.get(V.source);
  const card = say(`<div class="und"><small>Understood as</small>${[`measure: ${u.measure}`, `view: ${V.name.split(".").pop()}`, ...u.dims.map((d) => `by ${d}`), ...u.filters.map(([, v]) => `${v}`), u.window.label].map((x) => `<span>${esc(x)}</span>`).join("")}</div>
    <details class="sqlx"><summary>Metric view query → compiled SQL</summary><pre>${NexusHL.hl(c.mv, "sql")}</pre><pre>${NexusHL.hl(c.sql, "sql")}</pre></details><div class="ans">Running as ${esc(P.label)}…</div>`);
  try {
    const res = await lake.as(who, (l) => l.runSQL(c.sql));
    const rows = res.rows.map((r) => Object.fromEntries(res.columns.map((k, i) => [k, r[i]])));
    let sentence;
    if (!rows.length) sentence = "No data for that window.";
    else if (!u.dims.length) sentence = `${c.label} for ${u.window.label}${u.filters.length ? ` (${u.filters.map(([, v]) => v).join(", ")})` : ""}: <b>${show(rows[0][u.measure], c.unit)}</b>.`;
    else { const top = rows[0], d = u.dims[0].replace(" ", "_");
      sentence = u.dims.includes("month") ? `${c.label}, ${u.window.label}: from <b>${show(rows[0][u.measure], c.unit)}</b> in ${esc(String(rows[0].month).slice(0, 7))} to <b>${show(rows.at(-1)[u.measure], c.unit)}</b> in ${esc(String(rows.at(-1).month).slice(0, 7))}.`
        : `${c.label} by ${u.dims[0]}, ${u.window.label}: highest is <b>${esc(top[d])}</b> at <b>${show(top[u.measure], c.unit)}</b>${rows.length > 1 ? `, lowest <b>${esc(rows.at(-1)[d])}</b> at ${show(rows.at(-1)[u.measure], c.unit)}` : ""}.`; }
    const max = Math.max(...rows.map((r) => Math.abs(r[u.measure] || 0)), 1e-9);
    const chart = u.dims.length && rows.length > 1 ? `<div class="mini">${rows.slice(0, 12).map((r) => `<div><span>${esc(String(r[u.dims[0].replace(" ", "_")]).slice(0, 18))}</span><i style="--w:${Math.abs(r[u.measure] || 0) / max * 100}%" class="${r[u.measure] < 0 ? "neg" : ""}"></i><b>${show(r[u.measure], c.unit)}</b></div>`).join("")}</div>` : "";
    $(".ans", card).innerHTML = `<p>${sentence}</p>${chart}
      <p class="cite">Source: <code>${esc(V.source)}</code> · Delta v${t.current} · owner ${esc(OWNERS[u.view])}example.com · ${esc(V.measures[u.measure][3])}${P.rowFilter ? ` · <b>row filter applied: ${esc(P.rowFilter)} only</b>` : ""} · run as ${esc(P.label)}</p>`;
  } catch (e) {
    const denied = /INSUFFICIENT_PERMISSIONS/.test(e.message);
    $(".ans", card).innerHTML = denied
      ? `<p class="deny"><b>You don't have access to ${esc(V.source.split(".").pop())}.</b> I run with your permissions, so I can't see it either. Its readers are ${esc(lake.grantsFor(t).join(", "))}; ask ${esc(OWNERS[u.view])}example.com for access.</p>`
      : `<p class="deny">${esc(e.message)}</p>`;
  }
  $("#thread").scrollTop = $("#thread").scrollHeight;
}

const CHIPS = ["What were net flows last month by wrapper?", "What's our cost per acquisition by channel?", "How has AUA trended this year by month?", "Which function has the highest attrition?",
  "How long do transfers out take?", "Net flows for ISA in August", "What's our profit margin?", "List the vulnerable clients", "Should I move my ISA to cash?"];

(async () => {
  lake = await lakehouse($("#engine"));
  const [mx] = await lake.rows("SELECT max(month) AS m FROM nexus.gold.executive_kpis_monthly"); if (mx?.m) LATEST = String(mx.m).slice(0, 10);
  $("#engine").textContent = "Live lakehouse ready: answers run on the real Delta tables, with the asker's permissions.";
  const people = ["exec", "affluent", "finance", "marketing", "hr", "duty"];
  $("#who").innerHTML = people.map((k) => `<option value="${k}">${esc(lake.PERSONAS.find((p) => p.key === k).label)}</option>`).join("");
  $("#who").onchange = (e) => { who = e.target.value; say(`Now answering as <b>${esc(lake.PERSONAS.find((p) => p.key === who).label)}</b>. Same questions, different permissions.`, "bot sys"); };
  $("#chips").innerHTML = CHIPS.map((c) => `<button type="button">${esc(c)}</button>`).join("");
  $("#chips").querySelectorAll("button").forEach((b) => b.onclick = () => ask(b.textContent));
  $("#ask").onsubmit = (e) => { e.preventDefault(); const v = $("#q").value; $("#q").value = ""; ask(v); };
  const keys = Object.keys(VIEWS);
  $("#semtabs").innerHTML = keys.map((k) => `<button role="tab" data-k="${k}">${{ hr: "HR", exec: "Executive" }[k] || k[0].toUpperCase() + k.slice(1)}</button>`).join("");
  const showView = (k) => { $("#semcode").innerHTML = NexusHL.hl(yamlFor(k), "yaml"); $("#semtabs").querySelectorAll("button").forEach((b) => b.setAttribute("aria-selected", b.dataset.k === k)); };
  $("#semtabs").querySelectorAll("button").forEach((b) => b.onclick = () => showView(b.dataset.k));
  showView("finance");
})().catch((e) => { $("#engine").textContent = `Could not start: ${e.message}`; });
