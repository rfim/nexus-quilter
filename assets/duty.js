// Regulatory · Consumer Duty: a governed aggregate answers a question about vulnerable clients.
import { $, esc, fmt, header, lakehouse, evidence, choose, table, download } from "./jdkit.js";

header("duty.html");
lakehouse($("#engine")).then(() => { $("#engine").textContent = "Lakehouse ready: every query below runs on the real Delta tables, as the persona named on it."; });

const STEPS = ["access", "publish", "read", "act", "tell"];
const done = new Set();
const mark = (k) => { done.add(k); $("#progress").innerHTML = STEPS.map((s) => `<i class="${done.has(s) ? "done" : ""}"></i>`).join(""); };
mark("start");

const PRODUCT = "nexus.gold.consumer_duty_journey_outcomes";
const MIN_CELL = 10;
const AGG = `-- runs as the nexus-daily service principal, which can read the vulnerability flag
SELECT j.journey_type,
       CASE WHEN c.vulnerable THEN 'vulnerable' ELSE 'not vulnerable' END AS client_group,
       count(*) AS journeys,
       median(date_diff('day', CAST(j.created_on AS DATE), CAST(j.completed_on AS DATE))) AS median_days,
       quantile_disc(date_diff('day', CAST(j.created_on AS DATE), CAST(j.completed_on AS DATE)), 0.9) AS p90_days
FROM nexus.silver.service_journey j
JOIN nexus.silver.dim_client c USING (client_id)
WHERE j.completed_on IS NOT NULL
GROUP BY ALL
HAVING count(*) >= ${MIN_CELL}          -- minimum cell size from the contract
ORDER BY journey_type, client_group`;

const CONTRACT = `product: consumer_duty_journey_outcomes
owner:   head-of-consumer-duty@
purpose: Consumer Duty consumer support outcome. Do vulnerable clients
         get journeys completed as fast as other clients?
grain:   [journey_type, client_group]
privacy:
  source_columns: [dim_client.vulnerable]    # never leaves the pipeline
  min_cell_size:  {column: journeys, size: ${MIN_CELL}}
access:
  read: [grp_consumer_duty, grp_exec_mi]
tests:
  - not_null: [journey_type, client_group, median_days]
  - min_cell_size
monitors:
  - name:  vulnerability_gap
    rule:  median_days(vulnerable) / median_days(not vulnerable) <= 1.25
    on_breach: notify head-of-consumer-duty@, open a Duty review item`;

function render() {
  $("#flow").innerHTML = `
  <div class="card req"><span class="step">The question</span><h2>“For each service journey, do vulnerable clients wait longer?”</h2>
    <p>From the Head of Consumer Duty, for the annual board report on outcomes.</p>
    <dl><dt>What's sensitive</dt><dd>The vulnerability flag on dim_client is special category data in practice: only the platform and the DPO can read it.</dd>
    <dt>What they need</dt><dd>Journey times by journey type, split by vulnerable / not vulnerable. Never a list of clients.</dd></dl></div>

  <div class="card" id="s-access"><span class="step">Step 1 · Try the obvious way</span><h2>The analyst queries the client table directly</h2>
    <p>This is what usually happens first. It fails, and it should.</p><div id="ev-denied"></div>
    <div id="access-q"></div></div>

  <div class="card" id="s-publish" hidden><span class="step">Step 2 · Publish an aggregate from the pipeline</span><h2>The service principal builds the answer; people only see the totals</h2>
    <p>The pipeline is allowed to read the flag. It joins, aggregates, drops any group smaller than ${MIN_CELL} journeys so nobody can be singled out, and publishes the result with grants from the contract.</p>
    <div class="ev"><div class="ev-bar"><span><b>Pipeline job</b> · run as nexus-daily (service principal)</span><button class="small-btn solid" id="pub">Run and publish</button></div><pre>${NexusHL.hl(AGG, "sql")}</pre><div class="res" id="pub-res"></div></div>
    <h3>The contract that goes with it</h3><div class="diff">${NexusHL.hl(CONTRACT, "yaml")}</div></div>

  <div class="card" id="s-read" hidden><span class="step">Step 3 · Read it as the Consumer Duty analyst</span><h2>What the analyst sees</h2>
    <div id="ev-read"></div><div id="ev-still"></div><div id="bars"></div></div>

  <div class="card" id="s-act" hidden><span class="step">Step 4 · Decide</span><h2>Transfers out take twice as long for vulnerable clients. What now?</h2><div id="act"></div></div>
  <div class="card" id="s-tell" hidden><span class="step">Step 5 · Tell the right people</span><h2>Who hears about it first?</h2><div id="tell"></div></div>
  <div id="end"></div>`;

  evidence($("#ev-denied"), "As the Consumer Duty analyst", `SELECT vulnerable, count(*) AS clients
FROM nexus.silver.dim_client
GROUP BY ALL`, "duty").then((run) => run());
  choose($("#access-q"), [
    { t: "Ask the platform team to grant the analyst SELECT on dim_client", gain: "Quick.", cost: "Names, emails and NI numbers next to a vulnerability flag, open to someone who only needs totals. Hard to justify to the DPO." },
    { t: "Export the flag to a spreadsheet for them", gain: "Familiar.", cost: "An uncontrolled copy of the most sensitive data we have." },
    { t: "Publish an aggregate from the pipeline, with a minimum cell size, and grant only that", best: true,
      why: "The question is about groups, so the answer should be groups. The flag never leaves the pipeline, the analyst gets exactly what they need, and the DPO can see the rule in the contract.",
      gain: "The question gets answered; nobody new sees personal data.", cost: "A small product to own, with a contract and a monitor." },
  ], { onPick: () => { mark("access"); $("#s-publish").hidden = false; } });

  $("#pub").onclick = async () => {
    $("#pub").disabled = true; $("#pub-res").innerHTML = '<div class="err" style="color:#56665c">Running…</div>';
    try {
      const l = await lakehouse();
      await l.as("pipeline", (x) => x.publish(PRODUCT, AGG, { read: ["grp_consumer_duty", "grp_exec_mi"], comment: "Consumer Duty journey outcomes by vulnerability, min cell size 10" }));
      const res = await l.as("pipeline", (x) => x.runSQL(`DESCRIBE DETAIL ${PRODUCT}`));
      $("#pub-res").innerHTML = table(res);
      mark("publish"); $("#s-read").hidden = false; showRead();
    } catch (e) { $("#pub-res").innerHTML = `<div class="err">${esc(e.message)}</div>`; $("#pub").disabled = false; }
  };
}

async function showRead() {
  const run = await evidence($("#ev-read"), "As the Consumer Duty analyst", `SELECT * FROM ${PRODUCT}`, "duty");
  await run();
  const still = await evidence($("#ev-still"), "Same analyst, trying the client table again", `SELECT client_id, vulnerable FROM nexus.silver.dim_client LIMIT 5`, "duty");
  await still();
  const l = await lakehouse();
  const rows = await l.as("duty", (x) => x.rows(`SELECT * FROM ${PRODUCT}`));
  const by = {};
  for (const r of rows) (by[r.journey_type] ??= {})[r.client_group] = r;
  const max = Math.max(...rows.map((r) => r.median_days), 1);
  const gaps = Object.entries(by).filter(([, g]) => g.vulnerable && g["not vulnerable"]).map(([k, g]) => ({ k, v: g.vulnerable.median_days, n: g["not vulnerable"].median_days, ratio: g.vulnerable.median_days / Math.max(g["not vulnerable"].median_days, 0.5) }));
  $("#bars").innerHTML = `<h3>Median days to complete, by journey</h3><div class="alerts">${gaps.map((g) => `<div class="alert ${g.ratio > 1.25 ? "p1" : "ok"}">
    <b>${esc(g.k.replace("_", " "))}</b> <span class="pill ${g.ratio > 1.25 ? "red" : ""}">${g.ratio > 1.25 ? `gap ${g.ratio.toFixed(1)}×` : "no material gap"}</span>
    <div style="display:grid;grid-template-columns:110px 1fr;gap:4px 8px;align-items:center;margin-top:.4rem;font-size:.8rem">
      <span>not vulnerable</span><span><i style="display:inline-block;height:10px;border-radius:3px;background:#9fc7ad;width:${g.n / max * 100}%"></i> ${fmt(g.n, 1)} days</span>
      <span>vulnerable</span><span><i style="display:inline-block;height:10px;border-radius:3px;background:${g.ratio > 1.25 ? "#c0392b" : "#00875a"};width:${g.v / max * 100}%"></i> ${fmt(g.v, 1)} days</span></div></div>`).join("")}</div>
    <p class="muted" style="margin-top:.6rem">The monitor in the contract fires when a gap is above 1.25×. Groups under ${MIN_CELL} journeys are dropped before publishing.</p>`;
  mark("read"); $("#s-act").hidden = false;
  choose($("#act"), [
    { t: "Put the chart in the board report and move on", gain: "The report is on time.", cost: "Reports a harm without anyone owning the fix. The Duty asks firms to act on poor outcomes, as well as measure them." },
    { t: "Ask Platform Ops to prioritise vulnerable clients' transfers quietly", gain: "Might fix it.", cost: "No root cause, no evidence of the fix, and it bypasses the people accountable for the outcome." },
    { t: "Raise it as a potential poor outcome, find the cause with Ops, and track the gap until it closes", best: true,
      why: "First I'd check it's real: the gap holds across months and isn't driven by one provider. Then I'd break transfers out down by step with Ops to find where vulnerable clients wait (often the extra checks for third-party authority), and leave the monitor on so the board sees the gap close.",
      gain: "A harm found, owned, fixed and evidenced.", cost: "A week of analysis with Ops before there's a fix." },
  ], { onPick: () => { mark("act"); $("#s-tell").hidden = false; tell(); } });
}

function tell() {
  choose($("#tell"), [
    { t: "Post the chart in the analytics channel", gain: "Visible.", cost: "A potential conduct issue shared widely before its owner has seen it." },
    { t: "Email the Chief Executive", gain: "Gets attention.", cost: "Skips the accountable owner and the process they run." },
    { t: "The Head of Consumer Duty first, with the evidence and a proposed next step; then Platform Ops and the DPO", best: true,
      why: "The owner of the outcome decides how it's reported and escalated. Ops owns the fix. The DPO should know a new product uses the vulnerability flag, even in aggregate.",
      gain: "The right owner, in the right order, with evidence they can reproduce.", cost: "Nothing, apart from resisting the urge to share a finding." },
  ], { onPick: () => { mark("tell"); finish(); } });
}

function finish() {
  const note = `To: head-of-consumer-duty@
Cc: platform-ops-lead@, dpo@
Subject: Consumer Duty: vulnerable clients wait longer on transfers out

From the new consumer_duty_journey_outcomes product (aggregate only, groups under ${MIN_CELL} journeys removed):
transfers out take about twice as long for clients flagged as vulnerable, and withdrawals also take longer. Switches and transfers in show no material gap.

What I'd suggest: I work with Platform Ops this week to find which step causes the wait, and the gap monitor stays on, so the board report can show the trend.
The query, the contract and the Delta version are all recorded, so anyone can reproduce these numbers.

VIM`;
  $("#end").innerHTML = `<div class="card"><span class="step">Done</span><h2>The message I'd send</h2><div class="msg"><pre style="white-space:pre-wrap;margin:0;font:inherit">${esc(note)}</pre></div>
    <p style="margin-top:.8rem"><button class="small-btn solid" id="dl">Download the contract (YAML)</button></p></div>`;
  $("#dl").onclick = () => download("consumer_duty_journey_outcomes.yml", CONTRACT, "text/yaml");
  $("#end").scrollIntoView({ behavior: "smooth" });
}

render();
