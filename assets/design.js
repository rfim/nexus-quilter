// JD 1 · Technical Leadership: lead the design of a Consumer Duty data product, decision by decision.
import { $, esc, header, lakehouse, evidence, choose, download } from "./jdkit.js";

header("design.html");
lakehouse($("#engine")).then(() => { $("#engine").textContent = "Lakehouse ready: evidence queries run on the real Delta tables."; });

const REQUIREMENT = {
  title: "Consumer Duty: price and value",
  from: "Head of Consumer Duty, with Finance and Platform Ops",
  asks: [
    ["Board", "Quarterly: total cost of ownership against AUA, by client cohort and wrapper, with the history behind every number."],
    ["Platform Ops", "Intraday: today's flows by wrapper, to spot a problem before clients do."],
    ["Constraints", "Client data stays masked; Affluent and HNW see only their own clients; restatements must be explainable to an auditor."],
  ],
};

const DECISIONS = [
  { key: "cadence", title: "Batch, streaming or both?",
    context: "The board view is quarterly; Platform Ops wants today's picture. Corrections can arrive after the first publish.",
    evidence: ["How do corrections arrive today?", `-- the late batch is visible in the Delta history
SELECT count(*) FILTER (WHERE op = 'U') AS late_corrections,
       count(*) AS transactions
FROM nexus.bronze.platform_transaction`],
    options: [
      { t: "Daily batch for everything", gain: "One cadence, cheapest compute.", cost: "Platform Ops can't see today until tomorrow." },
      { t: "Stream everything end to end", gain: "Everything is fresh.", cost: "Always-on compute for a quarterly board view; harder to restate." },
      { t: "Hybrid: stream into bronze for the intraday view, scheduled batch for the certified board products", best: true,
        why: "Each consumer gets the freshness it needs, and the certified numbers come from one reproducible batch that can be restated when late corrections arrive.",
        gain: "Fresh ops view; reproducible board numbers.", cost: "Two cadences to monitor, each with its own SLO." },
    ] },
  { key: "model", title: "How do we model it?",
    context: "Advice, the platform and discretionary management each describe the same client their own way. The board wants cohorts and wrappers.",
    evidence: ["Does more than one channel describe the same clients?", `SELECT legal_entity, channel, count(*) AS clients
FROM nexus.silver.dim_client
GROUP BY ALL ORDER BY legal_entity, clients DESC`],
    options: [
      { t: "One wide table per report", gain: "Fast to build.", cost: "Definitions get copied report to report; no integration." },
      { t: "Kimball star in gold only", gain: "Simple, familiar for BI.", cost: "Integration of several sources happens inside dimension builds, and history of what we knew is lost." },
      { t: "Data Vault in silver to integrate sources, Kimball star in gold for consumption", best: true,
        why: "Hubs integrate several businesses by business key, insert-only satellites keep the audit history, and the board still reads a simple star. I'd only use the vault where there really are several sources.",
        gain: "Integration and audit history without forcing early agreement.", cost: "More tables; a vault is ceremony for single-source domains." },
    ] },
  { key: "build", title: "How do we build and run it?",
    context: "Rules for valid transactions already exist in the source contract. The team deploys with Azure DevOps.",
    evidence: ["What does the quality split catch today?", `SELECT rule, count(*) AS rows
FROM (SELECT unnest(_failed_rules) AS rule FROM nexus.silver.platform_transaction_quarantine)
GROUP BY rule ORDER BY rows DESC`],
    options: [
      { t: "Notebooks scheduled by hand", gain: "Quick to start.", cost: "No tests, no review, environments drift." },
      { t: "ADF data flows for transformations", gain: "Low code.", cost: "Rules live outside the contract and outside the code review." },
      { t: "Lakeflow declarative pipelines for silver (expectations from the contract), orchestrated as Workflows in an Asset Bundle", best: true,
        why: "Expectations come straight from the contract, quarantine keeps bad rows inspectable, and the bundle deploys the same code to dev, test and prod through the CI gate.",
        gain: "Rules as code; one deployment path.", cost: "The team needs Lakeflow and bundles skills (I'd pair on the first two)." },
    ] },
  { key: "serve", title: "How do we serve it?",
    context: "The board pack is built in Power BI. The organisation is adopting Fabric.",
    evidence: ["How big is the certified product?", "DESCRIBE DETAIL nexus.gold.finance_net_flows_daily"],
    options: [
      { t: "Copy gold into a Fabric Warehouse every night", gain: "Fabric-native.", cost: "A second copy to reconcile, and masks to re-implement." },
      { t: "Power BI import mode with its own measures", gain: "Fast visuals.", cost: "Measures redefined in DAX: two definitions of the same number." },
      { t: "Fabric reads gold in place (OneLake shortcut / mirrored catalog), Direct Lake semantic model over the metric view", best: true,
        why: "One copy and one definition. Before go-live I'd test how Unity Catalog masks and row filters carry into Fabric rather than assume they do.",
        gain: "No copy jobs; one definition of each measure.", cost: "Masked and filtered tables need a tested path into Fabric." },
    ] },
  { key: "access", title: "How do we protect it?",
    context: "Affluent and HNW must only see their own clients. Vulnerability flags must never be visible to analysts.",
    evidence: ["Who can see what? (run as the Affluent-only executive)", `SELECT legal_entity, round(sum(net_flows_gbp), 2) AS net_flows_gbp
FROM nexus.gold.finance_net_flows_daily
GROUP BY ALL`, "affluent"],
    options: [
      { t: "A separate table per legal entity", gain: "Simple to explain.", cost: "Every model duplicated; a new entity means new tables." },
      { t: "Rely on row-level security in Power BI", gain: "Familiar.", cost: "Anyone with notebook or SQL access bypasses it." },
      { t: "Unity Catalog: governed tags, column masks and a legal-entity row filter, with grants taken from the contract", best: true,
        why: "Protection lives with the data, so every tool (BI, notebooks, agents) gets the same rules. Try the Affluent-only persona above: HNW simply isn't there.",
        gain: "One control for every consumer.", cost: "Mask bindings must be a CI check until tag-based policies are standard." },
    ] },
  { key: "history", title: "How do we handle restatements?",
    context: "An auditor may ask what the board was shown last quarter, before late corrections.",
    evidence: ["How many published rows changed after the late batch?", `SELECT count(*) AS restated_rows
FROM nexus.gold.finance_net_flows_daily n
JOIN nexus.gold.finance_net_flows_daily VERSION AS OF 0 i USING (trade_date, wrapper_code, legal_entity)
WHERE n.net_flows_gbp <> i.net_flows_gbp`],
    options: [
      { t: "Overwrite gold nightly; keep nothing", gain: "Cheapest storage.", cost: "We can't say what we published." },
      { t: "Snapshot every table every day", gain: "Everything kept.", cost: "Storage grows fast, and nobody can find the right snapshot." },
      { t: "SCD2 for dimensions, Delta time travel within the retention window, vault satellites for longer history", best: true,
        why: "Each tool covers a different horizon: time travel answers 'what did we publish last week', satellites answer 'what did we know two years ago', and SCD2 keeps history consistent for reporting.",
        gain: "Reproducible numbers at any horizon.", cost: "Retention windows must be set deliberately and monitored." },
    ] },
];

const picks = {};
function progress() {
  $("#progress").innerHTML = DECISIONS.map((d) => `<i class="${picks[d.key] ? "done" : ""}"></i>`).join("");
}

function render() {
  $("#steps").innerHTML = `<div class="card req"><span class="step">The requirement</span><h2>${esc(REQUIREMENT.title)}</h2>
    <p>From: ${esc(REQUIREMENT.from)}</p><dl>${REQUIREMENT.asks.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join("")}</dl></div>` +
    DECISIONS.map((d, i) => `<div class="card" id="d-${d.key}"><span class="step">Decision ${i + 1} of ${DECISIONS.length}</span><h2>${esc(d.title)}</h2>
      <p>${esc(d.context)}</p><div class="evx"></div><div class="optx"></div></div>`).join("");
  for (const d of DECISIONS) {
    const card = $(`#d-${d.key}`);
    evidence($(".evx", card), d.evidence[0], d.evidence[1], d.evidence[2]);
    choose($(".optx", card), d.options, { onPick: (o) => { picks[d.key] = o; progress(); if (Object.keys(picks).length === DECISIONS.length) renderADR(); } });
  }
  progress();
}

function adrText() {
  const lines = [
    "# ADR-007: Consumer Duty price and value data product", "",
    `Status: Proposed · Author: VIM · Date: ${new Date().toISOString().slice(0, 10)}`, "",
    "## Context", "",
    "The board needs a quarterly view of total cost of ownership against AUA, by client cohort and wrapper, with the history behind every number. Platform Ops needs today's flows by wrapper. Client data must stay masked, each legal entity sees only its clients, and restatements must be explainable to an auditor.", "",
    "## Decisions", "",
  ];
  DECISIONS.forEach((d, i) => {
    const p = picks[d.key], best = d.options.find((o) => o.best);
    lines.push(`${i + 1}. **${d.title}** ${p.t}.`);
    if (p.gain) lines.push(`   - Gain: ${p.gain}`);
    if (p.cost) lines.push(`   - Cost: ${p.cost}`);
    if (!p.best) lines.push(`   - Reviewer note: consider "${best.t}". ${best.why}`);
  });
  lines.push("", "## Consequences", "",
    "- Each certified measure is defined once, with an owner, in a data contract; the CI gate blocks changes that move it by more than 0.5% without approval.",
    "- Freshness SLOs for the intraday view and the board products are monitored separately.",
    "- Before go-live: test how masks and row filters carry into Fabric; agree retention windows with the DPO.", "",
    "## Alternatives considered", "");
  DECISIONS.forEach((d) => d.options.filter((o) => o !== picks[d.key]).forEach((o) => lines.push(`- ${d.title} ${o.t}: ${o.cost || ""}`)));
  return lines.join("\n");
}

function renderADR() {
  const md = adrText();
  const matched = DECISIONS.filter((d) => picks[d.key].best).length;
  const html = md.split("\n").map((l) => l.startsWith("# ") ? `<h2>${esc(l.slice(2))}</h2>` : l.startsWith("## ") ? `<h3>${esc(l.slice(3))}</h3>`
    : l.trim().startsWith("- ") || /^\d+\./.test(l) ? `<li>${esc(l.replace(/^\s*(-|\d+\.)\s*/, "")).replace(/\*\*(.+?)\*\*/g, "<b>$1</b>")}</li>` : l ? `<p>${esc(l)}</p>` : "").join("");
  $("#adr").innerHTML = `<div class="card"><span class="step">Your design · ${matched} of ${DECISIONS.length} decisions matched mine</span>
    <h2>The ADR</h2><p>This is the record I'd raise for review before any code is written. Download it as Markdown.</p>
    <button class="small-btn solid" id="dl">Download ADR-007.md</button></div><div class="doc"><ul style="list-style:none;padding:0">${html}</ul></div>`;
  $("#dl").onclick = () => download("ADR-007-consumer-duty-price-and-value.md", md);
  $("#adr").scrollIntoView({ behavior: "smooth" });
}

render();
