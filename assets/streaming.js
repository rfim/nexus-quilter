// Skills · real-time: micro-batches over the landing feed, compared with the batch lakehouse.
import { $, esc, fmt, header, lakehouse, choose, table } from "./jdkit.js";

header("streaming.html");

const NET = new Set(["contribution", "withdrawal", "fee"]);
const TYPES = new Set(["contribution", "withdrawal", "fee", "switch_in", "switch_out", "income"]);
const valid = (e) => /^GB[0-9A-Z]{10}$/.test(e.isin) && TYPES.has(e.transaction_type);
const day = (d) => Date.parse(d) / 864e5;
let events = null, lake = null;

const say = (cls, text) => { const c = $("#console"); c.insertAdjacentHTML("beforeend", `<div class="${cls}">${esc(text)}</div>`); c.scrollTop = c.scrollHeight; };

async function boot() {
  lake = await lakehouse($("#engine"));
  const load = async (f) => (await (await fetch(`assets/sim/landing/${f}`)).text()).trim().split("\n").map((l) => JSON.parse(l));
  const [main, late] = await Promise.all([load("platform_transactions.jsonl"), load("platform_transactions_late.jsonl")]);
  // arrival order: the day's events in commit order, then the late corrections at the end
  main.sort((a, b) => a.trade_date.localeCompare(b.trade_date) || a.cdc_lsn - b.cdc_lsn);
  events = [...main, ...late];
  $("#engine").textContent = `Lakehouse ready · ${fmt(events.length)} events queued, replayed in the order the platform sent them`;
  $("#run").disabled = false;
}

const lit = (v) => v == null ? "NULL" : typeof v === "number" ? String(v) : `'${String(v).replace(/'/g, "''")}'`;

async function run() {
  $("#run").disabled = true; $("#after").innerHTML = ""; $("#console").innerHTML = "";
  const per = +document.querySelector('input[name="batch"]:checked').value;
  const sink = document.querySelector('input[name="sink"]:checked').value;
  const wmv = document.querySelector('input[name="wm"]:checked').value;
  const wm = wmv === "none" ? Infinity : +wmv;
  const conn = lake.conn;
  await conn.query("DROP TABLE IF EXISTS main.stream_bronze");
  await conn.query(`CREATE TABLE main.stream_bronze (transaction_id VARCHAR${sink === "merge" ? " PRIMARY KEY" : ""}, account_id VARCHAR, isin VARCHAR,
    transaction_type VARCHAR, amount_gbp DECIMAL(18,2), trade_date DATE, cdc_lsn BIGINT, op VARCHAR, wrapper_code VARCHAR)`);
  const cols = ["transaction_id", "account_id", "isin", "transaction_type", "amount_gbp", "trade_date", "cdc_lsn", "op", "wrapper_code"];
  say("muted", `readStream.format("eventhubs") · maxOffsetsPerTrigger=${per} · sink=${sink} · watermark=${wmv === "none" ? "none" : wm + " days"}`);

  // keyed streaming state for net flows: transaction_id -> last version seen
  const state = new Map();
  let total = 0, maxEvent = -Infinity, late = 0, quarantined = 0, dupSkipped = 0, batchNo = 0, worst = 0;
  for (let off = 0; off < events.length; off += per) {
    const batch = events.slice(off, off + per);
    const t0 = performance.now();
    const values = batch.map((e) => `(${cols.map((c) => lit(e[c])).join(",")})`).join(",");
    await conn.query(sink === "merge"
      ? `INSERT INTO main.stream_bronze VALUES ${values.replace(/\),\(/g, "),\n(")}
         ON CONFLICT (transaction_id) DO UPDATE SET account_id = excluded.account_id, isin = excluded.isin, transaction_type = excluded.transaction_type,
           amount_gbp = excluded.amount_gbp, trade_date = excluded.trade_date, cdc_lsn = excluded.cdc_lsn, op = excluded.op, wrapper_code = excluded.wrapper_code
         WHERE excluded.cdc_lsn > stream_bronze.cdc_lsn`
      : `INSERT INTO main.stream_bronze VALUES ${values}`).catch(async (err) => {
        // duplicate keys inside one micro-batch: dedupe the batch first, as you'd do before MERGE
        if (!/duplicate|conflict|ON CONFLICT/i.test(err.message)) throw err;
        const best = new Map();
        for (const e of batch) if (!best.has(e.transaction_id) || e.cdc_lsn > best.get(e.transaction_id).cdc_lsn) best.set(e.transaction_id, e);
        const v2 = [...best.values()].map((e) => `(${cols.map((c) => lit(e[c])).join(",")})`).join(",");
        await conn.query(`INSERT INTO main.stream_bronze VALUES ${v2} ON CONFLICT (transaction_id) DO UPDATE SET account_id = excluded.account_id, isin = excluded.isin,
          transaction_type = excluded.transaction_type, amount_gbp = excluded.amount_gbp, trade_date = excluded.trade_date, cdc_lsn = excluded.cdc_lsn,
          op = excluded.op, wrapper_code = excluded.wrapper_code WHERE excluded.cdc_lsn > stream_bronze.cdc_lsn`);
      });
    // stateful aggregate with a watermark on event time (trade_date)
    let lateHere = 0;
    for (const e of batch) {
      if (!valid(e)) { quarantined++; continue; }
      const t = day(e.trade_date);
      if (t < maxEvent - wm) { late++; lateHere++; continue; }
      const old = state.get(e.transaction_id);
      if (sink === "merge" && old && e.cdc_lsn <= old.cdc_lsn) { dupSkipped++; continue; }
      const val = (x) => x && NET.has(x.transaction_type) ? x.amount_gbp : 0;
      total += val(e) - (sink === "merge" ? val(old) : 0);
      state.set(e.transaction_id, e);
    }
    for (const e of batch) maxEvent = Math.max(maxEvent, day(e.trade_date));
    if (wm !== Infinity) for (const [k, v] of state) if (day(v.trade_date) < maxEvent - wm) state.delete(k);   // evict expired state
    const ms = Math.round(performance.now() - t0); worst = Math.max(worst, ms); batchNo++;
    const backlog = Math.max(0, events.length - off - batch.length);
    const wmDate = wm === Infinity ? "none" : new Date((maxEvent - wm) * 864e5).toISOString().slice(0, 10);
    say(lateHere ? "warn" : "ok", `batch ${String(batchNo).padStart(2)} · ${batch.length} events · ${ms} ms · watermark ${wmDate} · state ${fmt(state.size)} keys${lateHere ? ` · ${lateHere} late events dropped` : ""}`);
    $("#live").innerHTML = `<div><small>Micro-batches</small><b>${batchNo}</b></div><div><small>Backlog</small><b>${fmt(backlog)}</b></div>
      <div><small>Slowest batch</small><b>${worst} ms</b></div><div><small>Late events dropped</small><b>${late}</b></div>`;
    await new Promise((r) => setTimeout(r, Math.max(30, 2400 / Math.ceil(events.length / per))));
  }
  say("muted", `stream idle · ${quarantined} malformed events sent to quarantine · ${dupSkipped} replayed or stale events skipped by the state`);
  await compare(total, state.size, late, sink, wmv);
  $("#run").disabled = false; $("#run").textContent = "Run again with other settings";
}

async function compare(total, stateKeys, late, sink, wmv) {
  const conn = lake.conn;
  const s = (await conn.query("SELECT count(*) AS n, round(sum(amount_gbp), 2) AS amt FROM main.stream_bronze")).toArray()[0].toJSON();
  const [b] = await lake.rows("SELECT count(*) AS n, round(sum(amount_gbp), 2) AS amt FROM nexus.bronze.platform_transaction");
  const [g] = await lake.rows("SELECT round(sum(net_flows_gbp), 2) AS v FROM nexus.gold.finance_net_flows_daily");
  const ok = (a, c) => Math.abs(Number(a) - Number(c)) < 0.01;
  const rows = [
    ["Bronze rows", fmt(s.n), fmt(b.n), ok(s.n, b.n)],
    ["Bronze amount (£)", fmt(s.amt, 2), fmt(b.amt, 2), ok(s.amt, b.amt)],
    ["Net flows total (£)", fmt(total, 2), fmt(g.v, 2), ok(total, g.v)],
  ];
  $("#cmp").innerHTML = `<h3>Stream vs lakehouse</h3><p class="muted">Lakehouse figures are queried from the Delta tables: nexus.bronze.platform_transaction and the certified nexus.gold.finance_net_flows_daily.</p>
    <div class="tbl"><table><tr><th>Measure</th><th>Stream</th><th>Lakehouse</th><th></th></tr>${rows.map((r) => `<tr><td>${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td><td class="${r[3] ? "pass" : "fail"}">${r[3] ? "match" : "differs"}</td></tr>`).join("")}</table></div>
    <h3>Why</h3><ul class="timeline">
      ${sink === "append" ? `<li><b>Append</b> wrote every replayed event and every correction as new rows: ${fmt(s.n - b.n)} extra rows. The stream has no idea they are the same transactions.</li>` : `<li><b>MERGE</b> on transaction_id kept one row per transaction, and the newest cdc_lsn won, so replays did nothing and corrections replaced the original.</li>`}
      ${late ? `<li>The <b>${wmv}-day watermark</b> dropped ${late} late corrections from the aggregate: they changed trades from months ago, long after that state was evicted.</li>` : wmv === "none" ? `<li><b>No watermark</b> means every transaction stays in state: ${fmt(stateKeys)} keys, growing forever. Correct today, a memory problem next year.</li>` : ""}
      <li>The certified figure comes from batch: bronze is MERGEd, then gold is rebuilt with a 72-hour lookback and restated when corrections arrive.</li></ul>`;
  if (!$("#after").innerHTML) {
    $("#after").innerHTML = `<div class="card"><span class="step">The design call</span><h2>So how would you give Platform Ops real-time flows?</h2><div id="dc"></div></div>`;
    choose($("#dc"), [
      { t: "Stream everything to gold with a long watermark", gain: "One pipeline, near real time.", cost: "Huge state, and corrections months later still miss it. Two answers for the same certified number." },
      { t: "Keep batch only and refresh it every 15 minutes", gain: "One code path.", cost: "Rebuilding a year of gold four times an hour is wasteful, and it's still not live." },
      { t: "Stream into bronze with MERGE, serve Ops a live view with a short watermark labelled ‘provisional’, and keep the certified numbers on batch with restatement", best: true,
        why: "This page shows why: the stream gets bronze exactly right with MERGE, but a streaming aggregate can't absorb a correction from four months ago. So the stream does what it's good at (fresh, exactly-once bronze and a provisional live view) and batch keeps the certified figures reproducible.",
        gain: "Ops sees today; Finance's number never disagrees with itself.", cost: "Two cadences, each with its own SLO, and the ‘provisional’ label has to be visible." },
    ]);
  }
}

$("#run").onclick = () => run().catch((e) => { say("bad", e.message); $("#run").disabled = false; });
boot().catch((e) => { $("#engine").textContent = `Could not start: ${e.message}`; });
