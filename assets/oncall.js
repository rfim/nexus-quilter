// JD 5 · Operational Excellence: an on-call shift on the lakehouse.
import { $, esc, fmt, header, lakehouse, evidence, choose, table } from "./jdkit.js";

header("oncall.html");

const STEPS = ["triage", "investigate", "decide", "close", "tune"];
const done = new Set();
const mark = (k) => { done.add(k); $("#progress").innerHTML = STEPS.map((s) => `<i class="${done.has(s) ? "done" : ""}"></i>`).join(""); };
mark("start");

// a simulated clock: each action costs minutes, which become MTTA / MTTR
let clock = 7 * 60 + 2;
const log = [];
const hhmm = (m) => `${String(Math.floor(m / 60)).padStart(2, "0")}:${String(m % 60).padStart(2, "0")}`;
const tick = (mins, what) => { clock += mins; log.push([hhmm(clock), what]); };
let ackAt = null;

const RECON = `-- the failing contract check: platform net flows vs the general ledger, per day and wrapper
SELECT n.trade_date, n.wrapper_code,
       round(sum(n.net_flows_gbp), 2) AS platform_gbp,
       round(any_value(l.ledger_net_flows_gbp), 2) AS ledger_gbp,
       round(any_value(l.ledger_net_flows_gbp) - sum(n.net_flows_gbp), 2) AS difference_gbp
FROM nexus.gold.finance_net_flows_daily n
JOIN nexus.silver.finance_ledger l USING (trade_date, wrapper_code)
GROUP BY ALL
HAVING abs(difference_gbp) > 0.01`;

async function slo() {
  const l = await lakehouse($("#engine"));
  $("#engine").textContent = "Lakehouse ready: SLOs and every investigation query run on the real Delta tables.";
  const [f] = await l.rows("SELECT max(trade_date) AS latest, count(*) AS rows FROM nexus.gold.fct_platform_transaction");
  const [q] = await l.rows("SELECT (SELECT count(*) FROM nexus.silver.platform_transaction_quarantine) AS q, (SELECT count(*) FROM nexus.bronze.platform_transaction) AS b");
  const breaks = (await l.rows(RECON)).length;
  $("#slo").innerHTML = `<div><small>Freshness · gold</small><b>${esc(f.latest)}</b><br><span class="pill">within SLO</span></div>
    <div><small>Quarantine rate</small><b>${(q.q / q.b * 100).toFixed(2)}%</b><br><span class="pill">under 0.5%</span></div>
    <div><small>Reconciliation breaks</small><b>${breaks}</b><br><span class="pill ${breaks ? "red" : ""}">${breaks ? "SLO breached" : "within SLO"}</span></div>
    <div><small>Contract checks</small><b>39 / 40</b><br><span class="pill amber">1 failing</span></div>`;
}

function render() {
  $("#flow").innerHTML = `
  <div class="card"><span class="step">07:02 · Your alerts</span><h2>Which one do you pick up first?</h2>
    <div class="alerts">
      <div class="alert"><small>06:40 · volume_zscore · P3</small><br><b>Daily transaction volume outside 2σ of the 28-day average</b><br><small>It fires about twice a month. Nobody has ever found anything behind it.</small></div>
      <div class="alert p1"><small>06:58 · contract check · P1</small><br><b>finance_net_flows_daily: “reconcile to finance_ledger within £0.01” failed (1 row)</b><br><small>This product feeds the board pack and the executive KPIs. Finance refresh at 09:00.</small></div>
      <div class="alert ok"><small>07:01 · cluster · info</small><br><b>Job cluster autoscaled from 2 to 8 workers during the gold build</b><br><small>Run finished in 11 min (SLO 30 min).</small></div>
    </div><div id="triage"></div></div>
  <div class="card" id="s-inv" hidden><span class="step">Investigate</span><h2>What exactly failed, and why?</h2>
    <p>Three queries, run on the lakehouse. Each one costs a few minutes of the clock.</p>
    <div id="ev1"></div><div id="ev2"></div><div id="ev3"></div><div id="inv-fb"></div></div>
  <div class="card" id="s-dec" hidden><span class="step">Decide</span><h2>The ledger has £250 the platform never received. Finance refreshes at 09:00. What do you do?</h2><div id="dec"></div></div>
  <div class="card" id="s-close" hidden><span class="step">Incident record</span><h2 id="inc-title">INC-0412</h2><div id="inc"></div></div>
  <div class="card" id="s-tune" hidden><span class="step">Continuous improvement</span><h2>Now fix the alert that cries wolf</h2>
    <p>volume_zscore pages whoever's on call when a day's volume is far from its 28-day average. Move the threshold and see how often it would have paged over the year, computed on the real fact table.</p>
    <p><label>Threshold: <b id="zv">2.0</b>σ <input type="range" id="z" min="1.5" max="4" step="0.5" value="2" style="vertical-align:middle;accent-color:var(--green)"></label></p>
    <div id="chart"></div><div class="kv" id="zkv"></div><div id="tune"></div></div>
  <div id="end"></div>`;

  choose($("#triage"), [
    { t: "The volume alert: it arrived first", gain: "Clears the queue in order.", cost: "Spends the first half hour on the alert least likely to matter." },
    { t: "The failed reconciliation: it's a certified number with a 09:00 deadline", best: true,
      why: "Severity and deadline decide the order. A certified number feeding the board is wrong or unproven, and Finance reads it in two hours. The cluster event is information, and the volume alert can wait (and probably shouldn't be paging anyone).",
      gain: "The thing that can hurt is handled first.", cost: "The volume alert waits. That's fine." },
    { t: "Restart the whole pipeline and see if it goes away", gain: "Sometimes works.", cost: "Destroys the evidence, burns 11 minutes, and a reconciliation break won't fix itself." },
  ], { onPick: (o) => { ackAt = clock + 3; tick(3, `Acknowledged P1 (picked ${o.best ? "the reconciliation" : "another alert first"})`); if (!o.best) tick(25, "Lost time on a lower-priority alert"); mark("triage"); investigate(); } });
}

async function investigate() {
  $("#s-inv").hidden = false;
  const steps = [
    ["#ev1", "1 · Which day and wrapper break?", RECON, "Found the break: one day, one wrapper"],
    ["#ev2", "2 · Did we drop anything that day? (quarantine)", `SELECT trade_date, wrapper_code, transaction_id, amount_gbp, _failed_rules
FROM nexus.silver.platform_transaction_quarantine
WHERE trade_date = '2026-08-14'`, "Checked quarantine for 14 Aug"],
    ["#ev3", "3 · Is every bronze row for that day in gold?", `SELECT 'bronze (valid + quarantined)' AS layer, count(*) AS rows, round(sum(amount_gbp), 2) AS amount_gbp
FROM nexus.bronze.platform_transaction WHERE trade_date = '2026-08-14' AND wrapper_code = 'ISA'
UNION ALL
SELECT 'gold fact', count(*), round(sum(amount_gbp), 2)
FROM nexus.gold.fct_platform_transaction WHERE trade_date = '2026-08-14' AND wrapper_code = 'ISA'`, "Bronze and gold agree for 14 Aug ISA"],
  ];
  let ran = 0;
  for (const [sel, title, sql, what] of steps) {
    const run = await evidence($(sel), title, sql);
    $(".ev-bar button", $(sel)).onclick = async () => {
      const r = await run();
      if (r && !$(sel).dataset.done) { $(sel).dataset.done = 1; tick(4, what); ran++; }
      if (ran === steps.length && !$("#inv-fb").innerHTML) {
        $("#inv-fb").innerHTML = `<div class="fb"><b>What the evidence says.</b> Only 14 August, ISA, is off, by £250. Nothing was quarantined that day, and every bronze row is in gold. So the pipeline did its job: the ledger has a posting that the platform never sent us. The fault is upstream, between the platform and the ledger.</div>`;
        mark("investigate"); $("#s-dec").hidden = false; decide();
      }
    };
  }
}

function decide() {
  choose($("#dec"), [
    { t: "Add a £250 adjustment row to gold so the check passes before 09:00", gain: "Green by 09:00.", cost: "We'd invent a transaction we can't explain. That's how audit findings start." },
    { t: "Turn the reconciliation check off until it's understood", gain: "No more alert.", cost: "Removes the one control that caught this, for every other day too." },
    { t: "Keep the check red, publish with the break flagged, tell the Finance owner before 09:00, and raise it with the platform source team with the evidence", best: true,
      why: "The number is £250 out of £9.4m, so it's safe to use with a clear note, and Finance decides if that's acceptable. The fix belongs at the source: the platform team traces the missing transaction, and when it lands, the next run's MERGE corrects gold and the check goes green by itself.",
      gain: "Finance isn't surprised, the control stays on, and the real fix happens at the source.", cost: "The dashboard shows an amber flag for a day or two." },
  ], { onPick: (o) => { tick(8, o.best ? "Flagged the day on the product, messaged the Finance owner, raised PLAT-2291 with the evidence" : "Chose a workaround (reviewed later in the RCA)"); tick(12, "Finance owner confirmed: board pack goes ahead with the note"); mark("decide"); close(o); } });
}

function close(choice) {
  $("#s-close").hidden = false;
  const mtta = ackAt - (6 * 60 + 58), mttr = clock - (6 * 60 + 58);
  $("#inc").innerHTML = `<div class="kv"><div><small>Severity</small><b>P1</b></div><div><small>Time to acknowledge</small><b>${mtta} min</b></div><div><small>Time to mitigate</small><b>${mttr} min</b></div><div><small>Before 09:00?</small><b>${clock <= 9 * 60 ? "Yes" : "No"}</b></div></div>
    <h3>Timeline</h3><ul class="timeline"><li>06:58 Contract check failed on finance_net_flows_daily</li>${log.map(([t, w]) => `<li>${t} ${esc(w)}</li>`).join("")}</ul>
    <h3>Root cause (5 whys, short version)</h3><ul class="timeline">
      <li>The check failed because the ledger has £250 more than the platform for 14 Aug, ISA.</li>
      <li>The platform never sent a transaction that the ledger received, so it's a gap at the source, not in the pipeline.</li>
      <li>Nothing upstream compares what the platform sends with what the ledger books, so we found it a day late.</li></ul>
    <h3>Actions</h3><ul class="timeline">
      <li>Platform team: find and resend the missing transaction (PLAT-2291).</li>
      <li>Us: add a source-to-ledger completeness check at bronze, so the next gap shows up at ingestion.</li>
      <li>Us: a runbook entry for “reconciliation break, pipeline clean”, with these three queries.</li>
      ${choice.best ? "" : "<li>Review: the first decision would have hidden the break; the runbook now says why.</li>"}</ul>`;
  $("#inc-title").textContent = "INC-0412 · Reconciliation break, finance_net_flows_daily";
  mark("close"); $("#s-tune").hidden = false; tune();
}

let series = null;
async function tune() {
  const l = await lakehouse();
  series = await l.rows(`WITH d AS (
  SELECT trade_date, count(*) AS n FROM nexus.gold.fct_platform_transaction GROUP BY ALL)
SELECT trade_date, n,
       avg(n) OVER w AS mean_28d, stddev_pop(n) OVER w AS sd_28d
FROM d WINDOW w AS (ORDER BY trade_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING)
ORDER BY trade_date`);
  series = series.filter((r, i) => i >= 28).map((r) => ({ ...r, z: (r.n - r.mean_28d) / r.sd_28d }));
  const draw = () => {
    const z = +$("#z").value; $("#zv").textContent = z.toFixed(1);
    const fired = series.filter((r) => Math.abs(r.z) > z);
    const W = 900, H = 160, max = Math.max(...series.map((r) => r.n)) * 1.1;
    const x = (i) => i / (series.length - 1) * W, y = (v) => H - v / max * H;
    const path = series.map((r, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(r.n).toFixed(1)}`).join("");
    const band = series.map((r, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(r.mean_28d + z * r.sd_28d).toFixed(1)}`).join("")
      + series.slice().reverse().map((r, j) => `L${x(series.length - 1 - j).toFixed(1)},${y(Math.max(0, r.mean_28d - z * r.sd_28d)).toFixed(1)}`).join("") + "Z";
    $("#chart").innerHTML = `<svg class="spark" viewBox="0 0 ${W} ${H + 18}" role="img" aria-label="Daily transactions with the alert band">
      <path d="${band}" fill="#e7f1ea"/><path d="${path}" fill="none" stroke="#00875a" stroke-width="1.2"/>
      ${series.map((r, i) => Math.abs(r.z) > z ? `<circle cx="${x(i).toFixed(1)}" cy="${y(r.n).toFixed(1)}" r="3.5" fill="#c0392b"/>` : "").join("")}
      <text x="0" y="${H + 14}">${esc(series[0].trade_date)}</text><text x="${W}" y="${H + 14}" text-anchor="end">${esc(series.at(-1).trade_date)}</text></svg>`;
    $("#zkv").innerHTML = `<div><small>Pages in the year</small><b>${fired.length}</b></div><div><small>Real incidents behind them</small><b>0</b></div>
      <div><small>Nights disturbed (approx.)</small><b>${Math.round(fired.length * 0.6)}</b></div><div><small>Biggest swing</small><b>${fmt(Math.max(...series.map((r) => Math.abs(r.z))), 1)}σ</b></div>`;
  };
  $("#z").oninput = draw; draw();
  choose($("#tune"), [
    { t: "Raise the threshold to 4σ and keep paging", gain: "Fewer pages.", cost: "Still pages on a number with no owner and no action, and now it misses the real drops too." },
    { t: "Delete the alert", gain: "Quiet nights.", cost: "Loses a useful signal: a sudden fall in volume can mean a feed stopped." },
    { t: "Stop paging on it: send volume to a morning digest, and page only on SLO breaches with a runbook (freshness, completeness against the source, reconciliation)", best: true,
      why: "A page should mean someone has to act now and knows how. Volume swings are context for the morning; a missing feed shows up as a freshness or completeness breach, which already pages. I'd review every paging alert monthly against what it found.",
      gain: "Every page is actionable; the signal is kept.", cost: "One more monthly review, and the completeness check from the RCA has to land first." },
  ], { onPick: () => { mark("tune"); finish(); } });
}

function finish() {
  $("#end").innerHTML = `<div class="card"><span class="step">End of shift</span><h2>What I'd hand over at 17:00</h2>
    <div class="msg"><small>#nexus-platform · handover</small>P1 INC-0412 mitigated before the 09:00 refresh. Root cause is upstream: one ISA posting on 14 Aug that the platform never sent (PLAT-2291). Finance has been told and the day is flagged on the product. Check is still red on purpose; it'll go green when the platform resends. Follow-ups: a completeness check at bronze, a runbook entry, and volume_zscore moves from paging to the morning digest.</div></div>`;
  $("#end").scrollIntoView({ behavior: "smooth" });
}

render();
slo();
