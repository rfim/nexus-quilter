// JD 3 · Mentoring: review a junior engineer's PR, then coach.
import { $, esc, header, lakehouse, evidence } from "./jdkit.js";

header("review.html");
lakehouse($("#engine")).then(() => { $("#engine").textContent = "Lakehouse ready: the “prove it” query runs on the real bronze table."; });

const CODE = [
  "# src/nexus/jobs/load_adviser_fees.py",
  "from pyspark.sql import functions as F",
  "",
  'STORAGE_KEY = "Xk9q2…redacted…Lw=="',
  'spark.conf.set("fs.azure.account.key.nexusraw.dfs.core.windows.net", STORAGE_KEY)',
  "",
  "def load_adviser_fees(spark):",
  '    raw = spark.read.json("abfss://landing@nexusraw.dfs.core.windows.net/adviser_fees/")',
  "    fees = (raw",
  '        .withColumn("fee_gbp", F.col("amount").cast("double"))',
  '        .select("fee_id", "client_id", "email", "adviser_id", "fee_gbp", "fee_date"))',
  "",
  '    fees.write.mode("append").saveAsTable("nexus.gold.adviser_fees")',
  "",
  '    total = fees.agg(F.sum("fee_gbp")).collect()[0][0]',
  '    print(f"loaded {total}")',
  "    return fees.toPandas()",
  "",
  "# tests/  (no files changed)",
];

// line numbers are 1-based indexes into CODE
const ISSUES = [
  { lines: [4, 5], title: "A storage key in the code", severity: "blocker",
    comments: [
      { t: "Never commit secrets.", why: "True, but it doesn't say what to do instead, and the key is now in git history." },
      { t: "This key is now in git history, so I've asked the platform team to rotate it (no harm done, it happens). Could you read storage through the workspace's managed identity instead? There's no key to manage at all. Here's the pattern in bronze.py; happy to pair on it for 20 minutes.", best: true,
        why: "It deals with the risk first, without blame, gives the right pattern with an example, and offers time." },
      { t: "I'll fix this one myself.", why: "Quicker today, but Sam learns nothing and does it again next week." },
    ] },
  { lines: [11], title: "Client email copied into gold", severity: "blocker",
    comments: [
      { t: "PII!", why: "Correct and unhelpful." },
      { t: "Gold shouldn't carry email. Fees only need client_sk to join to the client dimension, and email is tagged as PII upstream, so analysts would only see a mask anyway. Could you drop email and client_id and use client_sk? The governance page in the wiki explains why we keep personal data out of gold.", best: true,
        why: "Explains the rule and the reason, gives the exact change, and points to where the standard is written down." },
      { t: "Ask the DPO whether this is OK.", why: "The standard already answers it; escalation isn't needed." },
    ] },
  { lines: [13], title: "Append means a replay double-counts", severity: "blocker",
    comments: [
      { t: "Use MERGE.", why: "Right answer, but Sam won't know why it matters." },
      { t: "If this job is re-run for the same day, append writes every fee twice and the totals double. Run the “prove it” query below to see it on real data. Could you MERGE on fee_id, like upsert_bronze() does? Then a re-run is always safe.", best: true,
        why: "Shows the consequence, lets Sam see it with real data, and links the house pattern." },
      { t: "Looks fine to me.", why: "It isn't: the first replay doubles adviser fees." },
    ] },
  { lines: [17], title: "toPandas() pulls every row to the driver", severity: "major",
    comments: [
      { t: "Don't use pandas.", why: "Too broad: pandas is fine on small results." },
      { t: "toPandas() brings the whole table to one machine, which will run out of memory once fees grow. The job doesn't need to return data, so could it just return the row count for logging? I can show you the Spark UI so you can see why.", best: true,
        why: "Names the failure mode, suggests the simpler fix, and offers to teach the tool behind it." },
      { t: "Approve, we can fix it when it breaks.", why: "It will break at night, on someone else's on-call." },
    ] },
  { lines: [19], title: "No tests", severity: "major",
    comments: [
      { t: "Add tests.", why: "Which tests, though?" },
      { t: "Could you add one test that runs the job twice on the same small input and checks the row count doesn't change? It's the test that would have caught the append issue. tests/test_bronze.py has one to copy.", best: true,
        why: "One specific, valuable test, tied to the bug it prevents, with an example to start from." },
      { t: "We don't have time for tests on this one.", why: "That's how the next incident starts." },
    ] },
];
const NIT = { lines: [16], title: "print() instead of logging", severity: "nit" };

const flagged = new Set();
function renderCode() {
  $("#code").innerHTML = CODE.map((l, i) => `<div class="ln${flagged.has(i + 1) ? " flag" : ""}" data-n="${i + 1}"><span>${i + 1}</span><span>${esc(l) || " "}</span></div>`).join("");
  document.querySelectorAll("#code .ln").forEach((el) => el.onclick = () => {
    const n = +el.dataset.n;
    if (!CODE[n - 1].trim()) return;
    flagged.has(n) ? flagged.delete(n) : flagged.add(n);
    renderCode();
  });
  $("#count").textContent = `${flagged.size} line(s) flagged`;
}

$("#submit").onclick = () => {
  $("#submit").disabled = true;
  const all = [...ISSUES, NIT];
  const found = all.filter((x) => x.lines.some((n) => flagged.has(n)));
  const issueLines = new Set(all.flatMap((x) => x.lines));
  const falseFlags = [...flagged].filter((n) => !issueLines.has(n));
  document.querySelectorAll("#code .ln").forEach((el) => {
    const n = +el.dataset.n;
    if (issueLines.has(n)) el.classList.add(flagged.has(n) ? "hit" : "miss");
    el.onclick = null;
  });
  $("#result").innerHTML = `<div class="card"><span class="step">Your review</span>
    <h2>You found ${found.filter((x) => x !== NIT).length} of ${ISSUES.length} real problems${found.includes(NIT) ? " and the nit" : ""}</h2>
    <p>${falseFlags.length ? `${falseFlags.length} flagged line(s) were fine as they are.` : "No false alarms."} Missed lines are shown in red in the code above.</p>
    <p>Now the part that makes it mentoring: for each problem, choose the comment you'd leave.</p></div>
    ${ISSUES.map((x, i) => `<div class="card" id="i-${i}"><span class="step">Line ${x.lines.join("–")} · <span class="pill ${x.severity === "blocker" ? "red" : "amber"}">${x.severity}</span></span>
      <h2>${esc(x.title)}</h2><div class="opts">${x.comments.map((c, k) => `<button data-k="${k}">${esc(c.t)}</button>`).join("")}</div><div class="fbx"></div>
      ${i === 2 ? '<div id="prove"></div>' : ""}</div>`).join("")}
    <div class="card"><span class="step">Nit</span><h2>print() instead of logging (line 16)</h2>
      <p>I'd mark this as “nit, optional” so it doesn't block the merge. New engineers can't tell blockers from preferences unless you label them.</p></div>
    <div id="score"></div>`;
  let answered = 0, coaching = 0;
  ISSUES.forEach((x, i) => {
    const card = $(`#i-${i}`);
    card.querySelectorAll(".opts button").forEach((b) => b.onclick = () => {
      const c = x.comments[+b.dataset.k];
      card.querySelectorAll(".opts button").forEach((y) => { y.disabled = true; if (x.comments[+y.dataset.k].best) y.classList.add("best"); });
      b.classList.add("picked");
      $(".fbx", card).innerHTML = `<div class="fb">${c.best ? "<b>That's how I'd put it.</b> " : "<b>Why I'd word it differently:</b> "}${esc(c.why)}</div>`;
      answered++; if (c.best) coaching++;
      if (answered === ISSUES.length) score(found.filter((y) => y !== NIT).length, coaching);
    });
  });
  evidence($("#prove"), "What a replay does to append vs MERGE (September's transactions)", `WITH batch AS (
  SELECT * FROM nexus.bronze.platform_transaction WHERE trade_date >= '2026-09-01'),
appended AS (SELECT * FROM batch UNION ALL SELECT * FROM batch),   -- the job ran twice
merged AS (SELECT * FROM appended
           QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY cdc_lsn DESC) = 1)
SELECT 'the batch itself' AS approach, count(*) AS rows, round(sum(amount_gbp), 2) AS total_gbp FROM batch
UNION ALL SELECT 'append, run twice', count(*), round(sum(amount_gbp), 2) FROM appended
UNION ALL SELECT 'MERGE on the key, run twice', count(*), round(sum(amount_gbp), 2) FROM merged`);
};

function score(found, coaching) {
  $("#score").innerHTML = `<div class="card"><span class="step">Done</span><h2>${found}/${ISSUES.length} problems found · ${coaching}/${ISSUES.length} comments that teach</h2>
    <p>What I'd do next with Sam: a 30-minute pairing session on MERGE and managed identity, then let them push the fixes themselves. Their next PR gets a lighter review if these patterns stick. At Lifepal, this mix of pairing from day one and clear review standards took onboarding from four weeks to two.</p></div>`;
  $("#score").scrollIntoView({ behavior: "smooth" });
}

renderCode();
