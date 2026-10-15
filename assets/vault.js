// Chapter 9 · Bring it together: Data Vault over three Quilter businesses, on the live lakehouse.
import { $, esc, fmt, header, lakehouse, table, choose } from "./jdkit.js";

let lake;
const run = (sql) => lake.runSQL(sql);
const rows = (sql) => lake.rows(sql);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const show = (sql, lang = "sql") => `<pre class="dv-sql">${NexusHL.hl(sql.trim(), lang)}</pre>`;
const maskNI = "concat(left(%s, 2), '••••••', right(%s, 1))";
const mask = (c) => maskNI.replaceAll("%s", c);

// ---- the three source systems, cut from the real client table ---------------------------------
const SOURCES = `-- runs as the pipeline service principal: three extracts, three keys, three views of a client
CREATE OR REPLACE TABLE src_platform AS                -- FNZ platform: account-centric
SELECT 'PLT-' || substr(c.client_id, 2) AS platform_ref, c.national_insurance_no AS ni_number,
       c.legal_entity, c.channel, count(a.account_id) AS accounts
FROM nexus.silver.dim_client c LEFT JOIN nexus.silver.account a USING (client_id)
GROUP BY ALL;
CREATE OR REPLACE TABLE src_advice AS                  -- Quilter Financial Planning: adviser CRM
SELECT 'QFP-' || lpad(CAST(hash(client_id) % 900000 AS VARCHAR), 6, '0') AS crm_id,
       lower(substr(national_insurance_no, 1, 2)) || ' ' || substr(national_insurance_no, 3)  AS ni_number,  -- typed by hand
       'Adviser ' || CAST(hash(client_id) % 40 + 1 AS VARCHAR) AS adviser,
       ['Defensive', 'Cautious', 'Balanced', 'Growth', 'Adventurous'][CAST(hash(client_id) % 5 AS BIGINT) + 1] AS risk_profile
FROM nexus.silver.dim_client WHERE channel = 'Quilter adviser';
CREATE OR REPLACE TABLE src_cheviot AS                 -- Quilter Cheviot: discretionary, high net worth
SELECT 'CHV-' || lpad(CAST(hash(client_id || 'c') % 90000 AS VARCHAR), 5, '0') AS cheviot_ref,
       national_insurance_no AS ni_number,
       'PM ' || CAST(hash(client_id) % 12 + 1 AS VARCHAR) AS portfolio_manager,
       CASE WHEN hash(client_id) % 3 = 0 THEN 'Advisory' ELSE 'Discretionary' END AS mandate
FROM nexus.silver.dim_client WHERE legal_entity = 'hnw'`;

const HUB = `-- the business key everyone shares: NI number, standardised, hashed
CREATE OR REPLACE TABLE hub_client AS
SELECT hk_client, bk_ni, min(load_ts) AS load_ts, arg_min(record_source, load_ts) AS record_source
FROM (
  SELECT md5(upper(replace(ni_number, ' ', ''))) AS hk_client, upper(replace(ni_number, ' ', '')) AS bk_ni,
         TIMESTAMP '2026-10-01 05:30' AS load_ts, 'fnz_platform' AS record_source FROM src_platform
  UNION ALL
  SELECT md5(upper(replace(ni_number, ' ', ''))), upper(replace(ni_number, ' ', '')),
         TIMESTAMP '2026-10-01 05:31', 'qfp_crm' FROM src_advice
  UNION ALL
  SELECT md5(upper(replace(ni_number, ' ', ''))), upper(replace(ni_number, ' ', '')),
         TIMESTAMP '2026-10-01 05:32', 'cheviot_dims' FROM src_cheviot)
GROUP BY hk_client, bk_ni`;

const LINKS = `-- relationships, each its own table: a client can gain an adviser without touching anything else
CREATE OR REPLACE TABLE link_client_adviser AS
SELECT md5(md5(upper(replace(ni_number, ' ', ''))) || adviser) AS hk_link,
       md5(upper(replace(ni_number, ' ', ''))) AS hk_client, md5(adviser) AS hk_adviser, adviser,
       TIMESTAMP '2026-10-01 05:31' AS load_ts, 'qfp_crm' AS record_source FROM src_advice;
CREATE OR REPLACE TABLE link_client_portfolio AS
SELECT md5(md5(ni_number) || cheviot_ref) AS hk_link, md5(ni_number) AS hk_client, md5(cheviot_ref) AS hk_portfolio,
       cheviot_ref, TIMESTAMP '2026-10-01 05:32' AS load_ts, 'cheviot_dims' AS record_source FROM src_cheviot`;

const SATS = `-- each source keeps its own description of the client, insert-only, with a hashdiff
CREATE OR REPLACE TABLE sat_client_platform AS
SELECT md5(ni_number) AS hk_client, TIMESTAMP '2026-10-01 05:30' AS load_ts,
       md5(concat_ws('|', legal_entity, channel, accounts)) AS hashdiff, legal_entity, channel, accounts, 'fnz_platform' AS record_source
FROM src_platform;
CREATE OR REPLACE TABLE sat_client_advice AS
SELECT md5(upper(replace(ni_number, ' ', ''))) AS hk_client, TIMESTAMP '2026-10-01 05:31' AS load_ts,
       md5(concat_ws('|', risk_profile, adviser)) AS hashdiff, risk_profile, adviser, 'qfp_crm' AS record_source
FROM src_advice;
CREATE OR REPLACE TABLE sat_client_cheviot AS
SELECT md5(ni_number) AS hk_client, TIMESTAMP '2026-10-01 05:32' AS load_ts,
       md5(concat_ws('|', mandate, portfolio_manager)) AS hashdiff, mandate, portfolio_manager, 'cheviot_dims' AS record_source
FROM src_cheviot`;

const DAY2 = `-- next morning: the CRM resends its full extract, and annual reviews changed some risk profiles
CREATE OR REPLACE TABLE src_advice_day2 AS
SELECT crm_id, ni_number, adviser,
       CASE WHEN hash(crm_id) % 8 = 0
            THEN ['Defensive', 'Cautious', 'Balanced', 'Growth', 'Adventurous'][(list_position(['Defensive', 'Cautious', 'Balanced', 'Growth', 'Adventurous'], risk_profile) % 5) + 1]
            ELSE risk_profile END AS risk_profile
FROM src_advice;
-- insert only what changed: compare the hashdiff with the latest row per client
INSERT INTO sat_client_advice
SELECT s.hk_client, TIMESTAMP '2026-10-02 05:31', s.hashdiff, s.risk_profile, s.adviser, 'qfp_crm'
FROM (SELECT md5(upper(replace(ni_number, ' ', ''))) AS hk_client, md5(concat_ws('|', risk_profile, adviser)) AS hashdiff, risk_profile, adviser
      FROM src_advice_day2) s
LEFT JOIN (SELECT hk_client, hashdiff FROM sat_client_advice
           QUALIFY row_number() OVER (PARTITION BY hk_client ORDER BY load_ts DESC) = 1) cur USING (hk_client)
WHERE cur.hashdiff IS DISTINCT FROM s.hashdiff`;

const PIT = (asOf) => `-- point in time: the latest row from each satellite as of ${asOf}, one row per client
WITH adv AS (SELECT * FROM sat_client_advice WHERE load_ts <= TIMESTAMP '${asOf}'
             QUALIFY row_number() OVER (PARTITION BY hk_client ORDER BY load_ts DESC) = 1),
     chv AS (SELECT * FROM sat_client_cheviot WHERE load_ts <= TIMESTAMP '${asOf}'
             QUALIFY row_number() OVER (PARTITION BY hk_client ORDER BY load_ts DESC) = 1),
     p AS (SELECT * FROM sat_client_platform WHERE load_ts <= TIMESTAMP '${asOf}'
           QUALIFY row_number() OVER (PARTITION BY hk_client ORDER BY load_ts DESC) = 1)
SELECT h.hk_client, p.legal_entity, p.channel, p.accounts,
       adv.risk_profile, adv.adviser, chv.mandate, chv.portfolio_manager,
       (adv.hk_client IS NOT NULL)::INT + (chv.hk_client IS NOT NULL)::INT + 1 AS businesses
FROM hub_client h
JOIN p USING (hk_client)
LEFT JOIN adv USING (hk_client) LEFT JOIN chv USING (hk_client)`;

// ---- the model diagram ------------------------------------------------------------------------
// node geometry (centres) for the diagram and the flow animation
const N = {
  src1: [75, 28, "FNZ platform", "PLT- refs · accounts", "src", "src_platform"], src2: [75, 123, "QFP adviser CRM", "QFP- ids · risk profile", "src", "src_advice"],
  src3: [75, 218, "Quilter Cheviot", "CHV- refs · mandates", "src", "src_cheviot"],
  satp: [230, 50, "sat_client_platform", "entity · channel · accounts", "sat", "sat_client_platform"], sata: [430, 50, "sat_client_advice", "risk profile · adviser", "sat", "sat_client_advice"],
  satc: [630, 50, "sat_client_cheviot", "mandate · portfolio manager", "sat", "sat_client_cheviot"],
  hub: [430, 125, "hub_client", "business key: NI number", "hub", "hub_client"],
  lnka: [230, 205, "link_client_adviser", "client ↔ adviser", "link", "link_client_adviser"], lnkp: [630, 205, "link_client_portfolio", "client ↔ Cheviot portfolio", "link", "link_client_portfolio"],
  mart: [790, 125, "dim_client_360", "Kimball, in gold", "mart", null],
};
const W2 = { src: 65, sat: 70, hub: 70, link: 70, mart: 60 };
const edgePath = (a, b) => { const [x1, y1, , , k1] = N[a], [x2, y2, , , k2] = N[b];
  const sx = x1 + (x2 > x1 ? W2[k1] : x2 < x1 ? -W2[k1] : 0), ex = x2 + (x2 > x1 ? -W2[k2] : x2 < x1 ? W2[k2] : 0);
  const sy = x1 === x2 ? y1 + (y2 > y1 ? 23 : -23) : y1, ey = x1 === x2 ? y2 + (y2 > y1 ? -23 : 23) : y2, m = (sx + ex) / 2;
  return `M${sx},${sy} C${m},${sy} ${m},${ey} ${ex},${ey}`; };
const EDGES = [["src1", "hub"], ["src2", "hub"], ["src3", "hub"], ["src1", "satp"], ["src2", "sata"], ["src3", "satc"], ["src2", "lnka"], ["src3", "lnkp"],
  ["satp", "hub"], ["sata", "hub"], ["satc", "hub"], ["hub", "lnka"], ["hub", "lnkp"], ["hub", "mart"], ["sata", "mart"], ["satc", "mart"]];
const FLOWS = { src: [], hub: ["src1>hub", "src2>hub", "src3>hub"], link: ["src2>lnka", "src3>lnkp", "hub>lnka", "hub>lnkp"], sat: ["src1>satp", "src2>sata", "src3>satc"],
  day2: ["src2>sata"], audit: ["sata>hub"], mart: ["hub>mart", "sata>mart", "satc>mart", "satp>hub"] };
let counts = {}, built = new Set();
const still = matchMedia("(prefers-reduced-motion: reduce)").matches;

function model(lit = new Set(), flows = []) {
  const fl = new Set(flows);
  const box = (id) => { const [x, y, label, sub, kind, tbl] = N[id], w = W2[kind] * 2;
    const c = tbl && counts[tbl] != null ? `${fmt(counts[tbl])} rows` : kind === "mart" && counts.mart != null ? `${fmt(counts.mart)} clients` : sub;
    const state = lit.has(id) || lit.has(kind) ? "on" : built.has(id) ? "built" : kind === "src" ? "" : "todo";
    return `<g class="dvn ${kind} ${state}"><rect x="${x - w / 2}" y="${y - 23}" width="${w}" height="46" rx="8"/><text x="${x}" y="${y - 3}" class="t1">${label}</text><text x="${x}" y="${y + 12}" class="t2">${c}</text></g>`; };
  const edges = EDGES.map(([a, b]) => { const d = edgePath(a, b), on = fl.has(`${a}>${b}`);
    return `<path d="${d}" class="dve ${on ? "on" : ""}"/>${on && !still ? [0, 0.45, 0.9].map((t) => `<circle r="3.6" class="dvdot"><animateMotion dur="1.35s" begin="-${t}s" repeatCount="indefinite" path="${d}"/></circle>`).join("") : ""}`; }).join("");
  $("#model").innerHTML = `<svg viewBox="0 0 860 245" role="img" aria-label="Data Vault model">${edges}${Object.keys(N).map(box).join("")}</svg>
    <div class="dv-legend"><span class="hub">Hub: one row per business key</span><span class="link">Link: a relationship</span><span class="sat">Satellite: one source's description, with history</span><span class="mart">Mart: what people query</span></div>`;
}
async function refreshCounts() {
  for (const t of Object.values(N).map((n) => n[5]).filter(Boolean)) {
    try { const [r] = await rows(`SELECT count(*) AS n FROM ${t}`); counts[t] = r.n; } catch { /* not built yet */ }
  }
}

// ---- the steps ----------------------------------------------------------------------------------
const STEPS = [
  { key: "src", lit: ["src"], title: "Three businesses, one client", sub: "Step 1 · Land the sources",
    text: "Each system has its own key: PLT-, QFP- and CHV- references. The only thing they share is the client's NI number, and the CRM stores it typed by hand, in lower case with a space.",
    sql: SOURCES, persona: "pipeline",
    after: async () => { const r = await run(`SELECT 'FNZ platform' AS source, count(*) AS clients, any_value(platform_ref) AS example_key, ${mask("any_value(ni_number)")} AS ni_as_stored FROM src_platform
      UNION ALL SELECT 'QFP adviser CRM', count(*), any_value(crm_id), ${mask("any_value(ni_number)")} FROM src_advice
      UNION ALL SELECT 'Quilter Cheviot', count(*), any_value(cheviot_ref), ${mask("any_value(ni_number)")} FROM src_cheviot`);
      return table(r) + `<p class="muted">NI numbers are masked on screen; the pipeline uses them only to build the hash key.</p>`; } },
  { key: "hub", lit: ["hub"], title: "One hub for the client", sub: "Step 2 · Build the hub",
    text: "The hub holds only the business key, standardised (upper case, no spaces) and hashed, plus when and where it was first seen. Without the standardising, the CRM's typing would create duplicate clients.",
    sql: HUB,
    after: async () => {
      const [h] = await rows(`SELECT count(*) AS hub, (SELECT count(*) FROM src_platform) + (SELECT count(*) FROM src_advice) + (SELECT count(*) FROM src_cheviot) AS source_rows,
        (SELECT count(DISTINCT md5(ni_number)) FROM src_advice) AS naive_extra,
        (SELECT count(*) - count(DISTINCT ni_number) FROM src_platform) AS shared_ni FROM hub_client`);
      const ov = await rows(`WITH k AS (SELECT md5(ni_number) hk, 1 p, 0 a, 0 c FROM src_platform UNION ALL SELECT md5(upper(replace(ni_number,' ',''))), 0,1,0 FROM src_advice UNION ALL SELECT md5(ni_number),0,0,1 FROM src_cheviot)
        SELECT (p + a + c)::INT AS n, count(*) AS clients FROM (SELECT hk, max(p) p, max(a) a, max(c) c FROM k GROUP BY hk) GROUP BY 1 ORDER BY 1`);
      const by = Object.fromEntries(ov.map((o) => [o.n, o.clients]));
      return `<div class="kv"><div><small>Rows from 3 sources</small><b>${fmt(h.source_rows)}</b></div><div><small>Clients in the hub</small><b>${fmt(h.hub)}</b></div>
        <div><small>In 2 businesses</small><b>${fmt(by[2] || 0)}</b></div><div><small>In all 3</small><b>${fmt(by[3] || 0)}</b></div></div>
        <p class="muted">Without standardising the key, the CRM's ${fmt(h.naive_extra)} hand-typed NI numbers would each have become a new “client”.</p>
        ${h.shared_ni ? `<p class="fb"><b>Data quality finding:</b> ${h.shared_ni} platform record(s) share an NI number with another record, so the hub holds ${fmt(h.hub)} clients, not ${fmt(h.hub + h.shared_ni)}. The vault doesn't hide this: it goes to the data owner as a same-as question, and the satellites keep both records' details.</p>` : ""}`; } },
  { key: "link", lit: ["link"], title: "Relationships as links", sub: "Step 3 · Build the links",
    text: "Who advises whom, and which Cheviot portfolio belongs to whom, each live in their own link table. A new relationship is a new row; nothing else is touched.",
    sql: LINKS,
    after: async () => table(await run("SELECT 'link_client_adviser' AS link, count(*) AS rows, count(DISTINCT hk_adviser) AS distinct_other_side FROM link_client_adviser UNION ALL SELECT 'link_client_portfolio', count(*), count(DISTINCT hk_portfolio) FROM link_client_portfolio")) },
  { key: "sat", lit: ["sat"], title: "Each source keeps its own view", sub: "Step 4 · Build the satellites",
    text: "There's no meeting to agree whose risk profile is right before loading: each source's description goes into its own satellite, with a hashdiff of its attributes so changes are cheap to spot. The three loads don't depend on each other, so they run in parallel.",
    sql: SATS, parallel: true,
    after: async () => table(await run("SELECT 'sat_client_platform' AS satellite, count(*) AS rows FROM sat_client_platform UNION ALL SELECT 'sat_client_advice', count(*) FROM sat_client_advice UNION ALL SELECT 'sat_client_cheviot', count(*) FROM sat_client_cheviot")) },
  { key: "day2", lit: ["src2", "sata"], title: "The next day: change arrives", sub: "Step 5 · Load a change, keep the history",
    text: "The CRM resends everything. Annual reviews changed some risk profiles. The load compares hashdiffs and inserts only real changes. Nothing is updated, nothing is deleted.",
    sql: DAY2,
    after: async () => { const [d] = await rows(`SELECT (SELECT count(*) FROM src_advice_day2) AS arrived, (SELECT count(*) FROM sat_client_advice WHERE load_ts = TIMESTAMP '2026-10-02 05:31') AS inserted`);
      return `<div class="kv"><div><small>Rows arrived</small><b>${fmt(d.arrived)}</b></div><div><small>Inserted (real changes)</small><b>${fmt(d.inserted)}</b></div><div><small>Updated</small><b>0</b></div><div><small>Deleted</small><b>0</b></div></div>`; } },
  { key: "audit", lit: ["hub", "sata"], title: "An auditor asks", sub: "Step 6 · Answer from history",
    text: "“What risk profile did we hold for this client on 1 October, and which system told us?” The satellite answers directly, with both versions and their source.",
    sql: `SELECT ${mask("h.bk_ni")} AS client, strftime(s.load_ts, '%Y-%m-%d %H:%M') AS loaded_at, s.risk_profile, s.adviser, s.record_source
FROM sat_client_advice s JOIN hub_client h USING (hk_client)
WHERE s.hk_client = (SELECT hk_client FROM sat_client_advice WHERE load_ts = TIMESTAMP '2026-10-02 05:31' ORDER BY hk_client LIMIT 1)
ORDER BY s.load_ts`, query: true },
  { key: "mart", lit: ["mart"], title: "One view for everyone else", sub: "Step 7 · Publish a Kimball dimension",
    text: "Most people never see the vault. A point-in-time query picks each source's latest row as of a date, and gold gets a simple client dimension: one row per client, across all three businesses.",
    sql: PIT("2026-10-02 23:59"),
    after: async () => {
      const [c] = await rows(`WITH a AS (${PIT("2026-10-01 23:59").replace(/^--[^\n]*\n/, "")}), b AS (${PIT("2026-10-02 23:59").replace(/^--[^\n]*\n/, "")})
        SELECT count(*) AS clients, count(*) FILTER (WHERE a.risk_profile IS DISTINCT FROM b.risk_profile) AS changed FROM a JOIN b USING (hk_client)`);
      const r = await run(`SELECT businesses, count(*) AS clients FROM (${PIT("2026-10-02 23:59").replace(/^--[^\n]*\n/, "")}) GROUP BY 1 ORDER BY 1`);
      return `<p>${fmt(c.clients)} clients in <code>dim_client_360</code>. As of 1 October vs 2 October, <b>${fmt(c.changed)}</b> have a different risk profile, and both answers stay reproducible.</p>${table(r)}`; } },
];

async function runStep(i) {
  const s = STEPS[i], card = $(`#st-${i}`), out = $(".dv-out", card), btn = $("button", card);
  btn.disabled = true; out.innerHTML = '<p class="muted">Running…</p>';
  model(new Set(s.lit), FLOWS[s.key]);
  try {
    if (s.parallel) {
      out.innerHTML = `<div class="dv-par">${["sat_client_platform", "sat_client_advice", "sat_client_cheviot"].map((n) => `<div><small>${n}</small><i><em></em></i></div>`).join("")}</div>`;
      const ems = out.querySelectorAll("em"); ems.forEach((e, k) => { e.style.transition = `width ${0.9 + k * 0.25}s ease`; requestAnimationFrame(() => e.style.width = "100%"); });
      await sleep(1300);
    }
    if (s.query) out.innerHTML = table(await run(s.sql));
    else {
      if (s.persona) await lake.as(s.persona, (l) => l.runSQL(s.sql)); else await run(s.sql);
      out.innerHTML = (s.parallel ? out.innerHTML : "") + await s.after();
    }
    await refreshCounts();
    const BUILT = { src: ["src1", "src2", "src3"], hub: ["hub"], link: ["lnka", "lnkp"], sat: ["satp", "sata", "satc"], mart: ["mart"] };
    (BUILT[s.key] || []).forEach((k) => built.add(k));
    if (s.key === "mart") { const [m] = await rows(`SELECT count(*) AS n FROM (${PIT("2026-10-02 23:59").replace(/^--[^\n]*\n/, "")})`); counts.mart = m.n; }
    model(new Set(), []);
    card.classList.add("done"); btn.textContent = "Done ✓";
    $("#prog").innerHTML = STEPS.map((_, k) => `<i class="${$(`#st-${k}`).classList.contains("done") ? "done" : ""}"></i>`).join("");
    if (i + 1 < STEPS.length) { $(`#st-${i + 1}`).hidden = false; } else finish();
  } catch (e) { out.innerHTML = `<div class="err">${esc(e.message)}</div>`; btn.disabled = false; }
}

function finish() {
  usesReady = true; renderUses();
  const el = document.createElement("div");
  el.className = "card"; el.innerHTML = `<span class="step">The judgement call</span><h2>Would you use a vault for every domain?</h2><div id="dc"></div>`;
  $("#dcwrap").replaceChildren(el);
  choose($("#dc"), [
    { t: "Yes: everything goes through the vault", gain: "One pattern everywhere.", cost: "Three extra tables for a single-source domain like HR, with nothing to integrate. Ceremony slows the team down." },
    { t: "No: Kimball straight from the sources, everywhere", gain: "Fewer tables, faster to build.", cost: "Integrating three businesses inside dimension builds, and losing what each system said and when." },
    { t: "Where several sources describe the same thing, or history must be auditable: vault in silver. Everywhere else: straight to Kimball", best: true,
      why: "Client, adviser and portfolio are described by three Quilter businesses and regulators can ask what we knew and when, so the vault earns its tables there. HR or marketing spend has one source, so it goes straight to a star. Either way gold looks the same to its users.",
      gain: "Integration and audit where they matter; simplicity elsewhere.", cost: "Two patterns to teach the team, so the decision rule is written down." },
  ]);
}

// ---- what the vault makes possible: three Quilter use cases ------------------------------------
const CLIENT_MAP = `c AS (SELECT md5(ni_number) AS hk_client, 'C' || substr(platform_ref, 5) AS client_id FROM src_platform)`;
const USES = [
  { key: "firms", title: "Adviser health: spot flows slowing before they leave", who: "Platform distribution · relationship managers · Finance",
    q: "Which advisers' clients are sending less money in this quarter than last?",
    lifepal: "Lifepal sat between insurers and customers, as Quilter sits between advisers and clients. Partner reporting on one agreed “bind” definition, with each insurer scoped to its own data, cut ad-hoc requests by 60%.",
    note: "The vault's link_client_adviser joins the adviser CRM to platform flows through the shared client hub. Without it, the two systems have no common key.",
    persona: "engineer", sql: `WITH ${CLIENT_MAP},
f AS (SELECT a.client_id,
             sum(amount_gbp) FILTER (WHERE trade_date >= DATE '2026-07-01') AS q3,
             sum(amount_gbp) FILTER (WHERE trade_date >= DATE '2026-04-01' AND trade_date < DATE '2026-07-01') AS q2
      FROM nexus.gold.fct_platform_transaction t JOIN nexus.silver.account a USING (account_id)
      WHERE transaction_type IN ('contribution', 'withdrawal', 'fee') GROUP BY ALL)
SELECT l.adviser, count(*) AS clients, round(sum(f.q2), 0) AS net_flows_q2, round(sum(f.q3), 0) AS net_flows_q3,
       round(100 * (sum(f.q3) - sum(f.q2)) / abs(sum(f.q2)), 1) AS change_pct
FROM link_client_adviser l JOIN c USING (hk_client) JOIN f USING (client_id)
GROUP BY ALL HAVING count(*) >= 10
ORDER BY change_pct LIMIT 8`,
    say: (r) => r.length ? `Early warning: <b>${esc(r[0].adviser)}</b>'s clients went from <b>${gbp(r[0].net_flows_q2)}</b> of net flows last quarter to <b>${gbp(r[0].net_flows_q3)}</b> this quarter. That's the conversation for the relationship manager this week.` : "" },
  { key: "duty", title: "Transfers out and Consumer Duty: fair journeys across the group", who: "Head of Consumer Duty · platform operations",
    q: "Do clients known to more than one Quilter business, or in vulnerable circumstances, wait longer to transfer out?",
    lifepal: "Insurer turnaround (application to decision) was the same kind of measure. Feeds arrived up to 72 hours late; CDC with idempotent MERGE took reporting from 24 hours to under 5 minutes.",
    note: "Runs as the pipeline service principal, which can read the vulnerability flag, and returns aggregates only, dropping any group under 10 journeys. The vault supplies how many businesses know each client.",
    persona: "pipeline", sql: `WITH ${CLIENT_MAP},
b AS (SELECT hk_client, 1 + (hk_client IN (SELECT hk_client FROM link_client_adviser))::INT
                          + (hk_client IN (SELECT hk_client FROM link_client_portfolio))::INT AS businesses FROM hub_client)
SELECT b.businesses, CASE WHEN d.vulnerable THEN 'vulnerable' ELSE 'not vulnerable' END AS client_group,
       count(*) AS transfers_out,
       median(date_diff('day', CAST(j.created_on AS DATE), CAST(j.completed_on AS DATE))) AS median_days
FROM nexus.silver.service_journey j
JOIN nexus.silver.dim_client d USING (client_id)
JOIN c USING (client_id) JOIN b USING (hk_client)
WHERE j.journey_type = 'transfer_out' AND j.completed_on IS NOT NULL
GROUP BY ALL HAVING count(*) >= 10
ORDER BY businesses, client_group`,
    say: (r) => { const g = (n, v) => r.find((x) => x.businesses === n && (x.client_group === "vulnerable") === v);
      const pairs = [1, 2, 3].map((n) => [n, g(n, true), g(n, false)]).filter(([, v, o]) => v && o);
      return pairs.length ? `Like for like, vulnerable clients wait longer: ${pairs.map(([n, v, o]) => `<b>${v.median_days} vs ${o.median_days} days</b> for clients known to ${n} business${n > 1 ? "es" : ""}`).join(", ")}. The Duty owner hears it first, with the evidence.` : ""; } },
  { key: "ai", title: "A governed AI assistant for advisers", who: "Quilter Financial Planning advisers · Compliance",
    q: "“How is my book doing?”, asked by one adviser, answered only from their own clients.",
    lifepal: "Definitions in the semantic layer first cut ad-hoc requests by 60%, and AI agents got read-only, role-scoped access to governed metadata, never a service account.",
    note: "The assistant would call this as a governed function with the adviser's own identity, so it can only ever see that adviser's clients. Here it runs for Adviser 26.",
    persona: "engineer", sql: `WITH ${CLIENT_MAP},
mine AS (SELECT hk_client FROM link_client_adviser WHERE adviser = 'Adviser 26'),
risk AS (SELECT hk_client, risk_profile FROM sat_client_advice
         QUALIFY row_number() OVER (PARTITION BY hk_client ORDER BY load_ts DESC) = 1),
flows AS (SELECT a.client_id, sum(amount_gbp) AS q3 FROM nexus.gold.fct_platform_transaction t
          JOIN nexus.silver.account a USING (account_id)
          WHERE transaction_type IN ('contribution', 'withdrawal', 'fee') AND trade_date >= DATE '2026-07-01' GROUP BY ALL)
SELECT r.risk_profile, count(*) AS clients, round(sum(coalesce(f.q3, 0)), 0) AS net_flows_q3
FROM mine JOIN risk r USING (hk_client) JOIN c USING (hk_client) LEFT JOIN flows f USING (client_id)
GROUP BY ALL ORDER BY clients DESC`,
    say: (r) => { const n = r.reduce((a, x) => a + x.clients, 0), f = r.reduce((a, x) => a + x.net_flows_q3, 0);
      return `“Your book: <b>${n} clients</b>, net flows of <b>${gbp(f)}</b> this quarter; most are ${esc(r[0]?.risk_profile || "")}.” Source: the vault and the certified fact, Delta-versioned, owner named. No advice, and no other adviser's clients.`; } },
];
let usesReady = false;
const gbp = (v) => `${v < 0 ? "−" : ""}£${fmt(Math.abs(v), 0)}`;
function renderUses() {
  $("#uses").innerHTML = `<div class="card dv-uses"><span class="step">What the vault makes possible</span><h2>Three Quilter use cases, each one query away</h2>
    <p>${usesReady ? "The vault is built. Run any of these on the live lakehouse." : "Build the vault above first (press <b>Build it</b>), then run these on the live lakehouse."}</p>
    <div class="use-grid">${USES.map((u, i) => `<div class="use ${usesReady ? "" : "locked"}" id="use-${i}"><span class="use-n">${i + 1}</span><h3>${esc(u.title)}</h3>
      <p class="use-q">${esc(u.q)}</p><p class="use-who">${esc(u.who)}</p>
      <p class="use-lp"><b>The Lifepal link.</b> ${esc(u.lifepal)}</p>
      <details><summary>The query</summary>${show(u.sql)}</details>
      <button class="small-btn solid" ${usesReady ? "" : "disabled"}>${usesReady ? "Run it on the vault" : "Build the vault first"}</button><div class="use-out"></div></div>`).join("")}</div></div>`;
  USES.forEach((u, i) => { $(`#use-${i} button`).onclick = async () => {
    const out = $(`#use-${i} .use-out`); out.innerHTML = '<p class="muted">Running…</p>';
    try {
      const res = await lake.as(u.persona, (l) => l.runSQL(u.sql));
      const rs = res.rows.map((r) => Object.fromEntries(res.columns.map((k, j) => [k, r[j]])));
      out.innerHTML = `<p class="use-say">${u.say(rs)}</p>${table(res)}<p class="muted">${esc(u.note)}</p>`;
    } catch (e) { out.innerHTML = `<div class="err">${esc(e.message)}</div>`; }
  }; });
}

/** The SQL behind each build step, for the Live lakehouse code browser. */
export const VAULT_SQL = { "01_sources.sql": SOURCES, "02_hub_client.sql": HUB, "03_links.sql": LINKS, "04_satellites.sql": SATS, "05_next_day_change.sql": DAY2, "06_pit_dim_client_360.sql": PIT("2026-10-02 23:59") };

/** Mount the whole simulation into `root`; used by the chapter page and the Live lakehouse tab. */
export function mountVault(root, l) {
  lake = l;
  counts = {}; built = new Set();
  usesReady = false;
  root.insertAdjacentHTML("beforeend", `<div class="dv-model card"><div class="dv-head"><div><span class="step">The model</span><h2>Hubs, links and satellites</h2></div>
    <button class="btn-run" id="dvplay">▶ Build it</button></div><p class="dv-now" id="dvnow">Press <b>Build it</b> to watch the vault being built on the live lakehouse, or run each step yourself below.</p><div id="model"></div></div>
    <div class="progress" id="prog"></div><div id="steps"></div><div id="dcwrap"></div><div id="uses"></div>`);
  renderUses();
  model();
  $("#steps").innerHTML = STEPS.map((s, i) => `<div class="card dv-step" id="st-${i}" ${i ? "hidden" : ""}><span class="step">${esc(s.sub)}</span><h2>${esc(s.title)}</h2><p>${esc(s.text)}</p>
    ${show(s.sql)}<button class="small-btn solid">${s.query ? "Ask the vault" : "Run it"}</button><div class="dv-out"></div></div>`).join("");
  STEPS.forEach((_, i) => $("button", $(`#st-${i}`)).onclick = () => runStep(i));
  $("#prog").innerHTML = STEPS.map(() => "<i></i>").join("");
  $("#dvplay").onclick = async () => {
    $("#dvplay").disabled = true;
    const stage = $("#model").closest(".dv-model");
    stage.classList.add("sticky");
    for (let i = 0; i < STEPS.length; i++) {
      const card = $(`#st-${i}`);
      if (card.classList.contains("done")) continue;
      card.hidden = false;
      card.scrollIntoView({ behavior: still ? "auto" : "smooth", block: "end" });
      $("#dvnow").innerHTML = `<b>${esc(STEPS[i].sub)}.</b> ${esc(STEPS[i].title)}`;
      model(new Set(STEPS[i].lit), FLOWS[STEPS[i].key]);
      await sleep(still ? 200 : 1700);
      await runStep(i);
      $("#dvnow").innerHTML = `<b>${esc(STEPS[i].sub)}.</b> ${esc(STEPS[i].title)} · <span class="ok">done</span>`;
      await sleep(still ? 200 : 900);
    }
    stage.classList.remove("sticky");
    $("#dvnow").innerHTML = `<b>Built.</b> ${fmt(counts.hub_client)} clients in the hub, three satellites with full history, and a Kimball view of ${fmt(counts.mart)} clients for everyone else.`;
    $("#dvplay").textContent = "Built ✓";
  };
}

if (document.body.dataset.page === "vault") {
  header("vault.html");
  lakehouse($("#engine")).then((l) => {
    $("#engine").textContent = "Live lakehouse ready: every step runs on the real Delta tables in your browser.";
    mountVault($("#vault"), l);
  }).catch((e) => { $("#engine").textContent = `Could not start: ${e.message}`; });
}
