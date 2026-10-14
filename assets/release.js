// JD 2 · Engineering Delivery: from a Finance request to a gated release.
import { $, esc, fmt, header, lakehouse, evidence, choose, table } from "./jdkit.js";

header("release.html");
lakehouse($("#engine")).then(() => { $("#engine").textContent = "Lakehouse ready: the gate below runs on the real Delta tables."; });

const CONTRACT_DIFF = `  product:     finance_net_flows_daily
- grain:       [legal_entity, wrapper_code, trade_date]
+ grain:       [legal_entity, wrapper_code, channel, trade_date]
  columns:
+   - name:  channel            # the client's channel on the trade date
+     tests: [not_null, {accepted_values: [Quilter adviser, Independent adviser, Direct]}]
  reconcile:
    against:   finance_ledger     # unchanged: totals must still match the ledger
  change:
    breaking:  owner_approval     # grain change → Finance signs off on the PR`;

const STEPS = ["clarify", "contract", "plan", "gate", "release"];
const done = new Set();
const mark = (k) => { done.add(k); $("#progress").innerHTML = STEPS.map((s) => `<i class="${done.has(s) ? "done" : ""}"></i>`).join(""); };
mark("start");

const QUESTIONS = [
  { t: "Which decision will this support, and by when?", good: true, why: "It sets the scope and the date: the adviser-channel review in two weeks." },
  { t: "Should channel be the client's channel when they traded, or their channel today?", good: true, why: "The point-in-time question. Clients move between advisers; the wrong choice silently rewrites history." },
  { t: "Must the channel totals add up to today's certified net flows?", good: true, why: "It becomes a gate check: the new grain must reconcile to the existing number." },
  { t: "What colour should the chart be?", why: "That can wait until the number is right." },
  { t: "Can we skip the tests to hit the date?", why: "The tests are what make the date safe to hit." },
  { t: "Shall I build a separate pipeline just for this?", why: "A second pipeline means a second definition of net flows." },
];

function render() {
  $("#flow").innerHTML = `
  <div class="card req"><span class="step">The request</span><h2>“Can we see net flows by distribution channel?”</h2>
    <p>From the Finance reporting lead: Quilter advisers, independent advisers and direct, for the adviser-channel review.</p></div>

  <div class="card" id="s-clarify"><span class="step">Step 1 · Clarify</span><h2>Pick the three questions you'd ask first</h2>
    <div class="opts" id="qs">${QUESTIONS.map((q, i) => `<button data-i="${i}">${esc(q.t)}</button>`).join("")}</div><div id="qfb"></div></div>

  <div class="card" id="s-contract" hidden><span class="step">Step 2 · Design as a contract change</span><h2>Change the contract, then see who it touches</h2>
    <p>Adding channel changes the product's grain, so it's a breaking change: the owner approves it on the pull request, and the blast radius is posted automatically.</p>
    <div class="diff">${NexusHL.hl(CONTRACT_DIFF, "diff:yaml")}</div>
    <h3>Blast radius (from lineage)</h3>
    <div style="overflow-x:auto"><table class="out-card" style="width:100%"><tbody>
      <tr><td>nexus.gold.executive_kpis_monthly</td><td>reads monthly net flows; its contract reconciles to Finance's</td><td>chief-data-officer@</td></tr>
      <tr><td>Board pack (Power BI)</td><td>net flows by wrapper; unaffected if it sums over channel</td><td>finance-reporting-lead@</td></tr>
      <tr><td>Metric view mv_client_flows</td><td>gains a Distribution Channel dimension</td><td>vim@</td></tr></tbody></table></div>
    <div id="ev-channel"></div>
    <button class="small-btn solid" id="to-plan">Plan it →</button></div>

  <div class="card" id="s-plan" hidden><span class="step">Step 3 · Plan</span><h2>Two weeks, three engineers, and one of them is new. Which plan?</h2><div id="plan"></div></div>

  <div class="card" id="s-gate" hidden><span class="step">Step 4 · Pull request and CI/CD gate</span><h2>Push the change through the gate</h2>
    <p>Two versions of the same pull request. One is right; one forgets that fees count towards net flows. Run the gate on each.</p>
    <div class="kv"><div><small>Pull request</small><b>#214</b></div><div><small>Target</small><b>main</b></div><div><small>Reviewers</small><b>VIM · Finance owner</b></div><div><small>Deploys</small><b>Asset Bundle</b></div></div>
    <p><label><input type="radio" name="ver" value="good" checked> Version A: <code>transaction_type IN ('contribution','withdrawal','fee')</code></label><br>
       <label><input type="radio" name="ver" value="bug"> Version B: <code>transaction_type IN ('contribution','withdrawal')</code></label></p>
    <button class="btn-run" id="rungate">Run the gate</button>
    <div id="gate" style="margin-top:1rem"></div></div>

  <div class="card" id="s-release" hidden><span class="step">Step 5 · Release</span><h2>Ship it, then tell Finance</h2><div id="rel"></div></div>`;

  // step 1
  const picked = new Set();
  $("#qs").querySelectorAll("button").forEach((b) => b.onclick = () => {
    if (picked.size >= 3 || picked.has(b.dataset.i)) return;
    picked.add(b.dataset.i); b.classList.add("picked");
    if (picked.size === 3) {
      $("#qs").querySelectorAll("button").forEach((x) => { x.disabled = true; if (QUESTIONS[+x.dataset.i].good) x.classList.add("best"); });
      const good = [...picked].filter((i) => QUESTIONS[i].good).length;
      $("#qfb").innerHTML = `<div class="fb"><b>${good} of 3 match mine.</b><ul>${QUESTIONS.map((q) => `<li>${q.good ? "✔" : "✗"} ${esc(q.t)} <span style="color:#56665c">${esc(q.why)}</span></li>`).join("")}</ul>
        <p>The answers: for the review on the 28th; <b>channel at the time of the trade</b>; and <b>yes, it must reconcile</b>.</p></div>`;
      mark("clarify"); $("#s-contract").hidden = false;
      evidence($("#ev-channel"), "Is channel already on the fact table, at trade time?", `SELECT channel, count(*) AS transactions, round(sum(amount_gbp), 2) AS amount_gbp
FROM nexus.gold.fct_platform_transaction
GROUP BY ALL ORDER BY transactions DESC`);
    }
  });
  $("#to-plan").onclick = () => {
    mark("contract"); $("#s-plan").hidden = false;
    choose($("#plan"), [
      { t: "Ship it in two days: add channel straight to gold, skip the contract change", gain: "Fast.", cost: "No owner sign-off on a grain change; the exec KPIs could drift unnoticed." },
      { t: "Contract PR → build behind the gate → backfill → one-week parallel run with Finance → cut over", best: true,
        why: "It fits in two weeks, the new engineer pairs on the build (good first change), and Finance sees both versions side by side before the old one goes. Risks named up front: point-in-time channel, reconciliation, the exec dependency.",
        gain: "Reviewed, reconciled, and Finance trusts it on day one.", cost: "About eight working days, not two." },
      { t: "Rebuild the Finance mart properly first (six weeks)", gain: "Cleaner long-term.", cost: "Misses the review entirely; a roadmap item, not this request." },
    ], { onPick: () => { mark("plan"); $("#s-gate").hidden = false; } });
  };
  $("#rungate").onclick = runGate;
}

async function runGate() {
  const ver = document.querySelector('input[name="ver"]:checked').value;
  const types = ver === "good" ? "'contribution','withdrawal','fee'" : "'contribution','withdrawal'";
  const g = $("#gate");
  const row = (name, status, detail) => `<div class="alert ${status === "pass" ? "ok" : status === "fail" ? "p1" : ""}"><b>${esc(name)}</b> <span class="pill ${status === "pass" ? "" : status === "fail" ? "red" : "amber"}">${status}</span><br><small>${detail}</small></div>`;
  g.innerHTML = `<div class="alerts">${row("ruff + mypy", "pass", "From the repo's CI on GitHub Actions")}${row("pytest (unit tests)", "pass", "5 passed: replay idempotence, SCD2 windows, contract compiler")}${row("databricks bundle validate -t test / -t prod", "pass", "Bundle definition valid for both targets")}${row("blast radius", "info", "3 downstream objects; owners added as reviewers")}<div id="diffrow">${row("metric diff", "running", "Computing on the lakehouse…")}</div></div>`;
  const l = await lakehouse();
  const [cand] = await l.rows(`SELECT round(sum(amount_gbp), 2) AS v FROM nexus.gold.fct_platform_transaction WHERE transaction_type IN (${types})`);
  const [cert] = await l.rows("SELECT round(sum(net_flows_gbp), 2) AS v FROM nexus.gold.finance_net_flows_daily");
  const byChannel = await l.runSQL(`SELECT channel, round(sum(amount_gbp), 2) AS net_flows_gbp
FROM nexus.gold.fct_platform_transaction WHERE transaction_type IN (${types}) GROUP BY ALL ORDER BY net_flows_gbp DESC`);
  const diff = Math.abs(cand.v - cert.v) / Math.abs(cert.v) * 100;
  const pass = diff <= 0.5;
  $("#diffrow").innerHTML = row("metric diff: net flows (candidate vs certified)", pass ? "pass" : "fail",
    `Certified £${fmt(cert.v, 2)} · candidate £${fmt(cand.v, 2)} · difference ${diff.toFixed(3)}% (threshold 0.5%).${pass ? " Channel totals reconcile to the certified number." : " Merge blocked: a certified metric moved. The candidate leaves out fees."}`)
    + `<div class="ev"><div class="ev-bar"><span><b>Candidate output</b> · net flows by channel</span></div><div class="res">${table(byChannel)}</div></div>`;
  g.insertAdjacentHTML("beforeend", `<p style="margin-top:.8rem">${pass ? '<span class="pill">Merge allowed</span> Finance approved the grain change on the PR.' : '<span class="pill red">Merge blocked</span> Fix the filter (try version A) and run the gate again.'}</p>`);
  if (pass) {
    mark("gate"); $("#s-release").hidden = false;
    const rows = byChannel.rows.map((r) => `${r[0]} £${fmt(r[1], 0)}`).join(" · ");
    $("#rel").innerHTML = `<div class="alerts">
      ${row("databricks bundle deploy -t prod", "pass", "Deployed from main by the pipeline; no manual changes")}
      ${row("backfill + parallel run (5 days)", "pass", "New and old products agree to the penny on every day")}
      ${row("cut over", "pass", "Old view retired after Finance confirmed")}</div>
      <h3>The note to Finance</h3><div class="msg"><small>To finance-reporting-lead@example.com</small><b>Net flows by channel is live</b>
      <p style="margin:.3rem 0 0">It's in the certified product now, at the client's channel on the trade date, and the totals reconcile to the existing net flows. Over the 12 months: ${esc(rows)}. The board pack is unchanged. The new dimension is also in self-service. If anything looks off, tell me and I'll trace it the same day.</p></div>`;
    mark("release");
  }
}

render();
