// Chapter 9 · Bring it together: three Data Vaults for Quilter, built on the live lakehouse.
//   A · Client 360           three businesses describe the same client
//   B · Adviser firms        firms, advisers and clients, with a tier change and an adviser moving firm
//   C · Transfers and Duty   journey status history, vulnerability in its own restricted satellite
import { $, esc, fmt, header, lakehouse, table, choose } from "./jdkit.js";

let lake;
const run = (sql) => lake.runSQL(sql);
const rows = (sql) => lake.rows(sql);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const show = (sql, lang = "sql") => `<pre class="dv-sql">${NexusHL.hl(sql.trim(), lang)}</pre>`;
const maskNI = "concat(left(%s, 2), '••••••', right(%s, 1))";
const mask = (c) => maskNI.replaceAll("%s", c);
const gbp = (v) => `${v < 0 ? "−" : ""}£${fmt(Math.abs(v), 0)}`;
const still = matchMedia("(prefers-reduced-motion: reduce)").matches;
const stripHead = (q) => q.replace(/^--[^\n]*\n/, "");

// =============================================================================================
// A · Client 360
// =============================================================================================
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

const CLIENT_MAP = `c AS (SELECT md5(ni_number) AS hk_client, 'C' || substr(platform_ref, 5) AS client_id FROM src_platform)`;
const A_STEPS = [
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

const A = {
  key: "a", button: "A · Client 360", title: "Three businesses, one client",
  intro: "Quilter's platform, its advice business and Quilter Cheviot each describe the same client with their own keys. The vault integrates them on the NI number, keeps every source's view with its history, and feeds one client dimension.",
  N: {
    src1: [75, 28, "FNZ platform", "PLT- refs · accounts", "src", "src_platform"], src2: [75, 123, "QFP adviser CRM", "QFP- ids · risk profile", "src", "src_advice"],
    src3: [75, 218, "Quilter Cheviot", "CHV- refs · mandates", "src", "src_cheviot"],
    satp: [230, 50, "sat_client_platform", "entity · channel · accounts", "sat", "sat_client_platform"], sata: [430, 50, "sat_client_advice", "risk profile · adviser", "sat", "sat_client_advice"],
    satc: [630, 50, "sat_client_cheviot", "mandate · portfolio manager", "sat", "sat_client_cheviot"],
    hub: [430, 125, "hub_client", "business key: NI number", "hub", "hub_client"],
    lnka: [230, 205, "link_client_adviser", "client ↔ adviser", "link", "link_client_adviser"], lnkp: [630, 205, "link_client_portfolio", "client ↔ Cheviot portfolio", "link", "link_client_portfolio"],
    mart: [790, 125, "dim_client_360", "Kimball, in gold", "mart", null],
  },
  EDGES: [["src1", "hub"], ["src2", "hub"], ["src3", "hub"], ["src1", "satp"], ["src2", "sata"], ["src3", "satc"], ["src2", "lnka"], ["src3", "lnkp"],
    ["satp", "hub"], ["sata", "hub"], ["satc", "hub"], ["hub", "lnka"], ["hub", "lnkp"], ["hub", "mart"], ["sata", "mart"], ["satc", "mart"]],
  FLOWS: { src: [], hub: ["src1>hub", "src2>hub", "src3>hub"], link: ["src2>lnka", "src3>lnkp", "hub>lnka", "hub>lnkp"], sat: ["src1>satp", "src2>sata", "src3>satc"],
    day2: ["src2>sata"], audit: ["sata>hub"], mart: ["hub>mart", "sata>mart", "satc>mart", "satp>hub"] },
  BUILT: { src: ["src1", "src2", "src3"], hub: ["hub"], link: ["lnka", "lnkp"], sat: ["satp", "sata", "satc"], mart: ["mart"] },
  PAR: ["sat_client_platform", "sat_client_advice", "sat_client_cheviot"],
  steps: A_STEPS,
  martCount: async () => (await rows(`SELECT count(*) AS n FROM (${stripHead(PIT("2026-10-02 23:59"))})`))[0].n, martUnit: "clients",
  files: { "01_sources.sql": SOURCES, "02_hub_client.sql": HUB, "03_links.sql": LINKS, "04_satellites.sql": SATS, "05_next_day_change.sql": DAY2, "06_pit_dim_client_360.sql": PIT("2026-10-02 23:59") },
  use: { key: "ai", title: "A governed AI assistant for advisers", who: "Quilter Financial Planning advisers · Compliance",
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
};

// =============================================================================================
// B · Adviser firms and flows
// =============================================================================================
const B_SOURCES = `-- three feeds: the adviser CRM, the firm register, and platform flows from the certified fact
CREATE OR REPLACE TABLE b_src_crm AS                       -- which adviser looks after which platform client
SELECT 'PLT-' || substr(client_id, 2) AS client_ref,
       'ADV-' || lpad(CAST(hash(client_id) % 40 + 1 AS VARCHAR), 3, '0') AS adviser_id,
       'Adviser ' || CAST(hash(client_id) % 40 + 1 AS VARCHAR) AS adviser_name
FROM nexus.silver.dim_client WHERE channel = 'Quilter adviser';
CREATE OR REPLACE TABLE b_src_register AS                  -- the firm register (fictional firms)
SELECT * FROM (VALUES
  ('FRM-01', 'Abbey Lane Wealth', 'Gold', 'London'),   ('FRM-02', 'Northgate Planning', 'Silver', 'Leeds'),
  ('FRM-03', 'Harbour and Vale', 'Silver', 'Bristol'), ('FRM-04', 'Kestrel Financial', 'Bronze', 'Glasgow'),
  ('FRM-05', 'Millbrook Partners', 'Gold', 'Southampton'), ('FRM-06', 'Ashdown Advice', 'Bronze', 'Brighton'),
  ('FRM-07', 'Fenwick Private', 'Silver', 'Edinburgh'), ('FRM-08', 'Larkfield Wealth', 'Bronze', 'Manchester')
) t(firm_ref, firm_name, tier, region);
CREATE OR REPLACE TABLE b_src_firm_adviser AS              -- which firm each adviser belongs to
SELECT adviser_id, 'FRM-0' || CAST(CAST(substr(adviser_id, 5) AS INT) % 8 + 1 AS VARCHAR) AS firm_ref
FROM (SELECT DISTINCT adviser_id FROM b_src_crm);
CREATE OR REPLACE TABLE b_src_flows AS                     -- monthly net flows per client, from the certified fact
SELECT 'PLT-' || substr(a.client_id, 2) AS client_ref, date_trunc('month', t.trade_date) AS month, round(sum(t.amount_gbp), 2) AS net_flows_gbp
FROM nexus.gold.fct_platform_transaction t JOIN nexus.silver.account a USING (account_id)
WHERE t.transaction_type IN ('contribution', 'withdrawal', 'fee') GROUP BY ALL`;
const B_HUBS = `-- three hubs, one per business key
CREATE OR REPLACE TABLE b_hub_firm AS
SELECT md5(firm_ref) AS hk_firm, firm_ref, TIMESTAMP '2026-10-01 05:30' AS load_ts, 'firm_register' AS record_source FROM b_src_register;
CREATE OR REPLACE TABLE b_hub_adviser AS
SELECT DISTINCT md5(adviser_id) AS hk_adviser, adviser_id, TIMESTAMP '2026-10-01 05:30' AS load_ts, 'qfp_crm' AS record_source FROM b_src_crm;
CREATE OR REPLACE TABLE b_hub_client AS
SELECT md5(client_ref) AS hk_client, client_ref, min(load_ts) AS load_ts, arg_min(src, load_ts) AS record_source FROM (
  SELECT client_ref, TIMESTAMP '2026-10-01 05:30' AS load_ts, 'qfp_crm' AS src FROM b_src_crm
  UNION ALL SELECT DISTINCT client_ref, TIMESTAMP '2026-10-01 05:31', 'fnz_platform' FROM b_src_flows) GROUP BY client_ref`;
const B_LINKS = `-- firm to adviser, adviser to client: each relationship its own link
CREATE OR REPLACE TABLE b_link_firm_adviser AS
SELECT md5(firm_ref || adviser_id) AS hk_link, md5(firm_ref) AS hk_firm, md5(adviser_id) AS hk_adviser,
       TIMESTAMP '2026-10-01 05:30' AS load_ts, 'qfp_crm' AS record_source FROM b_src_firm_adviser;
CREATE OR REPLACE TABLE b_link_adviser_client AS
SELECT md5(adviser_id || client_ref) AS hk_link, md5(adviser_id) AS hk_adviser, md5(client_ref) AS hk_client,
       TIMESTAMP '2026-10-01 05:30' AS load_ts, 'qfp_crm' AS record_source FROM b_src_crm`;
const B_SATS = `-- descriptions with history: firm details, and when each firm-adviser link is in effect
CREATE OR REPLACE TABLE b_sat_firm AS
SELECT md5(firm_ref) AS hk_firm, TIMESTAMP '2026-10-01 05:30' AS load_ts,
       md5(concat_ws('|', firm_name, tier, region)) AS hashdiff, firm_name, tier, region, 'firm_register' AS record_source
FROM b_src_register;
CREATE OR REPLACE TABLE b_eff_firm_adviser AS             -- effectivity satellite on the link
SELECT hk_link, TIMESTAMP '2026-10-01 05:30' AS load_ts, true AS is_current FROM b_link_firm_adviser;
CREATE OR REPLACE TABLE b_sat_client_flows AS             -- multi-active: one row per client per month
SELECT md5(client_ref) AS hk_client, month, net_flows_gbp, TIMESTAMP '2026-10-01 05:31' AS load_ts, 'fnz_platform' AS record_source
FROM b_src_flows`;
const B_DAY2 = `-- next morning: Harbour and Vale moves to Gold, and ADV-007 moves from Larkfield Wealth to Millbrook Partners
INSERT INTO b_sat_firm
SELECT md5('FRM-03'), TIMESTAMP '2026-10-02 05:30', md5(concat_ws('|', 'Harbour and Vale', 'Gold', 'Bristol')),
       'Harbour and Vale', 'Gold', 'Bristol', 'firm_register';
INSERT INTO b_link_firm_adviser
SELECT md5('FRM-05' || 'ADV-007'), md5('FRM-05'), md5('ADV-007'), TIMESTAMP '2026-10-02 05:30', 'qfp_crm';
INSERT INTO b_eff_firm_adviser VALUES
  (md5('FRM-08' || 'ADV-007'), TIMESTAMP '2026-10-02 05:30', false),   -- the old link ends
  (md5('FRM-05' || 'ADV-007'), TIMESTAMP '2026-10-02 05:30', true)     -- the new link starts`;
const B_FIRM_NOW = `cur AS (SELECT l.hk_firm, l.hk_adviser FROM b_link_firm_adviser l
             JOIN (SELECT hk_link, is_current FROM b_eff_firm_adviser
                   QUALIFY row_number() OVER (PARTITION BY hk_link ORDER BY load_ts DESC) = 1) e USING (hk_link)
             WHERE e.is_current),
     tier AS (SELECT hk_firm, firm_name, tier FROM b_sat_firm
              QUALIFY row_number() OVER (PARTITION BY hk_firm ORDER BY load_ts DESC) = 1)`;
const B_MART = `-- gold: net flows per firm and quarter, through today's firm for each adviser and the firm's current tier
WITH ${B_FIRM_NOW}
SELECT t.firm_name, t.tier, count(DISTINCT cur.hk_adviser) AS advisers,
       round(sum(f.net_flows_gbp) FILTER (WHERE f.month >= DATE '2026-04-01' AND f.month < DATE '2026-07-01'), 0) AS net_flows_q2,
       round(sum(f.net_flows_gbp) FILTER (WHERE f.month >= DATE '2026-07-01'), 0) AS net_flows_q3
FROM cur JOIN tier t USING (hk_firm)
JOIN b_link_adviser_client ac USING (hk_adviser)
JOIN b_sat_client_flows f ON f.hk_client = ac.hk_client
GROUP BY ALL ORDER BY net_flows_q3 DESC`;
const B = {
  key: "b", button: "B · Adviser firms", title: "Firms, advisers and their clients' money",
  intro: "Flows reach the platform through adviser firms. The vault links firms to advisers and advisers to clients, keeps a firm's tier history, and records when an adviser moves firm, so flows can be read by firm as they really were.",
  N: {
    src1: [75, 40, "QFP adviser CRM", "adviser ↔ client", "src", "b_src_crm"], src2: [75, 125, "Firm register", "tier · region", "src", "b_src_register"],
    src3: [75, 210, "FNZ platform", "monthly net flows", "src", "b_src_flows"],
    hubf: [250, 125, "hub_firm", "key: FRM- ref", "hub", "b_hub_firm"], satf: [250, 40, "sat_firm", "name · tier · region", "sat", "b_sat_firm"],
    lfa: [430, 125, "link_firm_adviser", "firm ↔ adviser", "link", "b_link_firm_adviser"], eff: [430, 40, "eff_firm_adviser", "when the link is in effect", "sat", "b_eff_firm_adviser"],
    huba: [610, 125, "hub_adviser", "key: ADV- id", "hub", "b_hub_adviser"], lac: [610, 210, "link_adviser_client", "adviser ↔ client", "link", "b_link_adviser_client"],
    hubc: [430, 210, "hub_client", "key: PLT- ref", "hub", "b_hub_client"], satcf: [250, 210, "sat_client_flows", "net flows by month", "sat", "b_sat_client_flows"],
    mart: [790, 125, "fct_firm_flows", "Kimball, in gold", "mart", null],
  },
  EDGES: [["src2", "hubf"], ["src1", "huba"], ["src1", "hubc"], ["src3", "hubc"], ["hubf", "lfa"], ["lfa", "huba"], ["huba", "lac"], ["lac", "hubc"],
    ["src2", "satf"], ["satf", "hubf"], ["eff", "lfa"], ["src3", "satcf"], ["satcf", "hubc"], ["huba", "mart"]],
  FLOWS: { src: [], hub: ["src2>hubf", "src1>huba", "src1>hubc", "src3>hubc"], link: ["hubf>lfa", "lfa>huba", "huba>lac", "lac>hubc"],
    sat: ["src2>satf", "eff>lfa", "src3>satcf"], day2: ["src2>satf", "eff>lfa"], audit: ["eff>lfa", "lfa>huba"], mart: ["satcf>hubc", "lac>hubc", "huba>mart"] },
  BUILT: { src: ["src1", "src2", "src3"], hub: ["hubf", "huba", "hubc"], link: ["lfa", "lac"], sat: ["satf", "eff", "satcf"], mart: ["mart"] },
  PAR: ["sat_firm", "eff_firm_adviser", "sat_client_flows"],
  steps: [
    { key: "src", lit: ["src"], sub: "Step 1 · Land the sources", title: "Three feeds about firms, advisers and money",
      text: "The adviser CRM says who looks after whom. The firm register says which firm an adviser works for, and its tier. The platform's certified fact gives each client's monthly net flows.",
      sql: B_SOURCES,
      after: async () => table(await run("SELECT 'adviser CRM' AS source, count(*) AS rows FROM b_src_crm UNION ALL SELECT 'firm register', count(*) FROM b_src_register UNION ALL SELECT 'firm ↔ adviser', count(*) FROM b_src_firm_adviser UNION ALL SELECT 'platform flows (client × month)', count(*) FROM b_src_flows")) },
    { key: "hub", lit: ["hub"], sub: "Step 2 · Build the hubs", title: "One hub per business key",
      text: "Firm, adviser and client each get a hub: just the key, hashed, with when and where it was first seen.",
      sql: B_HUBS,
      after: async () => table(await run("SELECT 'hub_firm' AS hub, count(*) AS keys FROM b_hub_firm UNION ALL SELECT 'hub_adviser', count(*) FROM b_hub_adviser UNION ALL SELECT 'hub_client', count(*) FROM b_hub_client")) },
    { key: "link", lit: ["link"], sub: "Step 3 · Build the links", title: "Who works where, who looks after whom",
      text: "Two links carry the relationships. When an adviser moves firm, that's a new link row; the old one stays as history.",
      sql: B_LINKS,
      after: async () => table(await run("SELECT 'link_firm_adviser' AS link, count(*) AS rows FROM b_link_firm_adviser UNION ALL SELECT 'link_adviser_client', count(*) FROM b_link_adviser_client")) },
    { key: "sat", lit: ["sat"], sub: "Step 4 · Build the satellites", title: "Tier history, link effectivity, monthly flows",
      text: "The firm's details carry a hashdiff. An effectivity satellite on the firm–adviser link says when each relationship is in force. Flows are a multi-active satellite: one row per client per month.",
      sql: B_SATS, parallel: true,
      after: async () => table(await run("SELECT 'sat_firm' AS satellite, count(*) AS rows FROM b_sat_firm UNION ALL SELECT 'eff_firm_adviser', count(*) FROM b_eff_firm_adviser UNION ALL SELECT 'sat_client_flows', count(*) FROM b_sat_client_flows")) },
    { key: "day2", lit: ["src2", "satf", "lfa", "eff"], sub: "Step 5 · Load a change, keep the history", title: "A firm moves up a tier; an adviser moves firm",
      text: "Harbour and Vale is upgraded to Gold, and ADV-007 moves from Larkfield Wealth to Millbrook Partners. Two inserts and an effectivity change. Nothing is updated in place.",
      sql: B_DAY2,
      after: async () => { const [d] = await rows("SELECT (SELECT count(*) FROM b_sat_firm) AS sat_rows, (SELECT count(*) FROM b_link_firm_adviser) AS links, (SELECT count(*) FROM b_eff_firm_adviser) AS eff_rows");
        return `<div class="kv"><div><small>sat_firm rows</small><b>${fmt(d.sat_rows)}</b></div><div><small>firm ↔ adviser links</small><b>${fmt(d.links)}</b></div><div><small>effectivity rows</small><b>${fmt(d.eff_rows)}</b></div><div><small>Updated or deleted</small><b>0</b></div></div>`; } },
    { key: "audit", lit: ["lfa", "eff", "huba"], sub: "Step 6 · Answer from history", title: "Which firm was ADV-007 with, and when?",
      text: "A relationship manager asks why ADV-007's flows moved between firms. The link and its effectivity satellite answer with dates.",
      sql: `SELECT a.adviser_id, s.firm_name, strftime(e.load_ts, '%Y-%m-%d %H:%M') AS from_load, e.is_current
FROM b_eff_firm_adviser e
JOIN b_link_firm_adviser l USING (hk_link)
JOIN b_hub_adviser a USING (hk_adviser)
JOIN (SELECT hk_firm, firm_name FROM b_sat_firm QUALIFY row_number() OVER (PARTITION BY hk_firm ORDER BY load_ts DESC) = 1) s USING (hk_firm)
WHERE a.adviser_id = 'ADV-007'
ORDER BY e.load_ts, e.is_current`, query: true },
    { key: "mart", lit: ["mart"], sub: "Step 7 · Publish a Kimball fact", title: "Net flows by firm, for everyone else",
      text: "Gold gets a simple fact: net flows per firm per quarter, through each adviser's current firm and the firm's current tier.",
      sql: B_MART,
      after: async () => table(await run(stripHead(B_MART))) },
  ],
  martCount: async () => (await rows(`SELECT count(*) AS n FROM (${stripHead(B_MART)})`))[0].n, martUnit: "firms",
  files: { "01_sources.sql": B_SOURCES, "02_hubs.sql": B_HUBS, "03_links.sql": B_LINKS, "04_satellites.sql": B_SATS, "05_next_day_change.sql": B_DAY2, "06_fct_firm_flows.sql": B_MART },
  use: { title: "Adviser health: spot flows slowing before they leave", who: "Platform distribution · relationship managers · Finance",
    q: "Which firms' clients are sending less money this quarter than last?",
    lifepal: "Lifepal sat between insurers and customers, as Quilter sits between adviser firms and clients. Partner reporting on one agreed “bind” definition, with each insurer scoped to its own data, cut ad-hoc requests by 60%.",
    note: "The firm view only exists because the vault links firm → adviser → client, and remembers when an adviser moved.",
    persona: "engineer", sql: `WITH m AS (${stripHead(B_MART)})
SELECT firm_name, tier, advisers, net_flows_q2, net_flows_q3,
       round(100 * (net_flows_q3 - net_flows_q2) / abs(net_flows_q2), 1) AS change_pct
FROM m ORDER BY change_pct`,
    say: (r) => r.length ? `Early warning: <b>${esc(r[0].firm_name)}</b> (${esc(r[0].tier)}) went from <b>${gbp(r[0].net_flows_q2)}</b> to <b>${gbp(r[0].net_flows_q3)}</b> quarter on quarter. That's this week's conversation for its relationship manager.` : "" },
};

// =============================================================================================
// C · Transfers and Consumer Duty
// =============================================================================================
const C_SOURCES = `-- runs as the pipeline service principal: journey events from the platform, and the vulnerability flag
CREATE OR REPLACE TABLE c_src_events AS                    -- status events, as the platform emits them
SELECT journey_id, client_id, journey_type, 'requested' AS status, CAST(created_on AS DATE) AS event_date FROM nexus.silver.service_journey
UNION ALL
SELECT journey_id, client_id, journey_type, 'in_progress', CAST(created_on AS DATE) + 1 FROM nexus.silver.service_journey
WHERE completed_on IS NOT NULL AND CAST(completed_on AS DATE) > CAST(created_on AS DATE) + 1
UNION ALL
SELECT journey_id, client_id, journey_type, 'completed', CAST(completed_on AS DATE) FROM nexus.silver.service_journey
WHERE completed_on IS NOT NULL;
CREATE OR REPLACE TABLE c_src_clients AS                   -- special category in practice: never leaves the pipeline
SELECT client_id, vulnerable FROM nexus.silver.dim_client`;
const C_HUBS = `-- a hub for the journey (the request) and one for the client
CREATE OR REPLACE TABLE c_hub_journey AS
SELECT DISTINCT md5(journey_id) AS hk_journey, journey_id, TIMESTAMP '2026-10-01 05:30' AS load_ts, 'fnz_platform' AS record_source FROM c_src_events;
CREATE OR REPLACE TABLE c_hub_client AS
SELECT DISTINCT md5(client_id) AS hk_client, client_id, TIMESTAMP '2026-10-01 05:30' AS load_ts, 'fnz_platform' AS record_source FROM c_src_events`;
const C_LINKS = `-- which client made which request
CREATE OR REPLACE TABLE c_link_client_journey AS
SELECT DISTINCT md5(client_id || journey_id) AS hk_link, md5(client_id) AS hk_client, md5(journey_id) AS hk_journey,
       TIMESTAMP '2026-10-01 05:30' AS load_ts, 'fnz_platform' AS record_source FROM c_src_events`;
const C_SATS = `-- status history is insert-only, and vulnerability gets its own satellite with its own grants
CREATE OR REPLACE TABLE c_sat_journey_status AS
SELECT md5(journey_id) AS hk_journey, event_date, status, journey_type, TIMESTAMP '2026-10-01 05:30' AS load_ts, 'fnz_platform' AS record_source
FROM c_src_events;
CREATE OR REPLACE TABLE c_sat_client_vulnerability AS     -- GRANT SELECT only to the pipeline and the DPO
SELECT md5(client_id) AS hk_client, vulnerable, TIMESTAMP '2026-10-01 05:30' AS load_ts, 'qfp_crm' AS record_source
FROM c_src_clients`;
const C_DAY2 = `-- next morning: the platform corrects 25 transfer-out completions, which actually finished 3 days later
INSERT INTO c_sat_journey_status
SELECT hk_journey, event_date + 3, 'completed', journey_type, TIMESTAMP '2026-10-02 05:30', 'fnz_correction'
FROM c_sat_journey_status
WHERE status = 'completed' AND journey_type = 'transfer_out'
QUALIFY row_number() OVER (ORDER BY hash(hk_journey)) <= 25`;
const C_LATEST = (known) => `latest AS (SELECT * FROM c_sat_journey_status WHERE load_ts <= TIMESTAMP '${known}'
                QUALIFY row_number() OVER (PARTITION BY hk_journey, status ORDER BY load_ts DESC) = 1)`;
const C_MART = `-- gold, run as the pipeline: transfer-out outcomes by vulnerability, aggregates only, groups under 10 dropped
WITH ${C_LATEST("2026-10-02 23:59")},
j AS (SELECT r.hk_journey, r.journey_type, date_diff('day', r.event_date, c.event_date) AS days
      FROM latest r JOIN latest c ON c.hk_journey = r.hk_journey AND c.status = 'completed'
      WHERE r.status = 'requested')
SELECT j.journey_type, CASE WHEN v.vulnerable THEN 'vulnerable' ELSE 'not vulnerable' END AS client_group,
       count(*) AS journeys, median(j.days) AS median_days, quantile_disc(j.days, 0.9) AS p90_days
FROM j JOIN c_link_client_journey l USING (hk_journey) JOIN c_sat_client_vulnerability v USING (hk_client)
GROUP BY ALL HAVING count(*) >= 10
ORDER BY journey_type, client_group`;
const C = {
  key: "c", button: "C · Transfers and Consumer Duty", title: "Every request, every status, kept",
  intro: "Consumer Duty asks whether clients can act as easily as they join, and whether vulnerable clients do as well as others. The vault keeps every status a request passes through, takes late corrections without losing what was first reported, and keeps vulnerability in its own locked-down satellite.",
  N: {
    src1: [75, 60, "FNZ journey events", "requested · in progress · done", "src", "c_src_events"], src2: [75, 190, "Client records", "vulnerability flag 🔒", "src", "c_src_clients"],
    hubj: [290, 60, "hub_journey", "key: journey id", "hub", "c_hub_journey"], hubc: [290, 190, "hub_client", "key: client id", "hub", "c_hub_client"],
    lnk: [470, 125, "link_client_journey", "client ↔ request", "link", "c_link_client_journey"],
    satj: [600, 40, "sat_journey_status", "status history, insert-only", "sat", "c_sat_journey_status"],
    satv: [600, 210, "sat_client_vulnerability", "restricted: pipeline + DPO", "sat", "c_sat_client_vulnerability"],
    mart: [790, 125, "journey_outcomes", "aggregate, in gold", "mart", null],
  },
  EDGES: [["src1", "hubj"], ["src1", "hubc"], ["src2", "hubc"], ["hubj", "lnk"], ["hubc", "lnk"], ["src1", "satj"], ["satj", "hubj"], ["src2", "satv"], ["satv", "hubc"], ["lnk", "mart"], ["satj", "mart"], ["satv", "mart"]],
  FLOWS: { src: [], hub: ["src1>hubj", "src1>hubc", "src2>hubc"], link: ["hubj>lnk", "hubc>lnk"], sat: ["src1>satj", "src2>satv"],
    day2: ["src1>satj"], audit: ["satj>hubj"], mart: ["lnk>mart", "satj>mart", "satv>mart"] },
  BUILT: { src: ["src1", "src2"], hub: ["hubj", "hubc"], link: ["lnk"], sat: ["satj", "satv"], mart: ["mart"] },
  PAR: ["sat_journey_status", "sat_client_vulnerability"],
  steps: [
    { key: "src", lit: ["src"], sub: "Step 1 · Land the sources", title: "Status events and a sensitive flag",
      text: "The platform emits an event each time a request changes status. The vulnerability flag comes from client records and only the pipeline may read it.",
      sql: C_SOURCES, persona: "pipeline",
      after: async () => table(await run("SELECT status, count(*) AS events FROM c_src_events GROUP BY ALL ORDER BY events DESC")) + '<p class="muted">The flag itself is never shown: only the pipeline reads c_src_clients.</p>' },
    { key: "hub", lit: ["hubj", "hubc"], sub: "Step 2 · Build the hubs", title: "The request and the client",
      text: "A transfer request is a business object in its own right, so it gets a hub, as does the client who made it.",
      sql: C_HUBS,
      after: async () => table(await run("SELECT 'hub_journey' AS hub, count(*) AS keys FROM c_hub_journey UNION ALL SELECT 'hub_client', count(*) FROM c_hub_client")) },
    { key: "link", lit: ["lnk"], sub: "Step 3 · Build the link", title: "Who asked for what",
      text: "One link joins each request to its client.",
      sql: C_LINKS,
      after: async () => table(await run("SELECT 'link_client_journey' AS link, count(*) AS rows FROM c_link_client_journey")) },
    { key: "sat", lit: ["satj", "satv"], sub: "Step 4 · Build the satellites", title: "Status history, and a locked-down satellite",
      text: "Every status event is a new row. Vulnerability sits in its own satellite so it can carry tighter grants than everything else: splitting satellites by sensitivity is one of the vault's quiet strengths.",
      sql: C_SATS, parallel: true, persona: "pipeline",
      after: async () => table(await run("SELECT 'sat_journey_status' AS satellite, count(*) AS rows FROM c_sat_journey_status UNION ALL SELECT 'sat_client_vulnerability', count(*) FROM c_sat_client_vulnerability")) },
    { key: "day2", lit: ["src1", "satj"], sub: "Step 5 · Load a correction, keep the history", title: "The platform corrects 25 completions",
      text: "25 transfers out actually completed three days later than first reported. The corrections are inserted next to the original events: what we first reported is still there.",
      sql: C_DAY2,
      after: async () => { const [d] = await rows("SELECT count(*) FILTER (WHERE record_source = 'fnz_correction') AS corrections, count(*) AS rows FROM c_sat_journey_status");
        return `<div class="kv"><div><small>Corrections inserted</small><b>${fmt(d.corrections)}</b></div><div><small>Status rows now</small><b>${fmt(d.rows)}</b></div><div><small>Updated</small><b>0</b></div><div><small>Deleted</small><b>0</b></div></div>`; } },
    { key: "audit", lit: ["satj", "hubj"], sub: "Step 6 · Answer from history", title: "What did we report, and what was true?",
      text: "The Duty owner asks about the 25 corrected transfers: how long did we first say they took, and how long did they really take? Both answers come straight from the satellite.",
      sql: `WITH fix AS (SELECT DISTINCT hk_journey FROM c_sat_journey_status WHERE record_source = 'fnz_correction'),
${C_LATEST("2026-10-01 23:59").replace("latest AS", "v1 AS")},
${C_LATEST("2026-10-02 23:59").replace("latest AS", "v2 AS")}
SELECT 'as first reported (1 Oct)' AS view, count(*) AS corrected_transfers,
       round(avg(date_diff('day', r.event_date, c.event_date)), 1) AS avg_days
FROM v1 r JOIN v1 c USING (hk_journey) JOIN fix USING (hk_journey)
WHERE r.status = 'requested' AND c.status = 'completed'
UNION ALL
SELECT 'after the correction (2 Oct)', count(*), round(avg(date_diff('day', r.event_date, c.event_date)), 1)
FROM v2 r JOIN v2 c USING (hk_journey) JOIN fix USING (hk_journey)
WHERE r.status = 'requested' AND c.status = 'completed'`, query: true },
    { key: "mart", lit: ["mart"], sub: "Step 7 · Publish an aggregate", title: "Journey outcomes for the Duty owner",
      text: "Gold gets aggregates only, built by the pipeline, with groups under 10 dropped, so nobody downstream can see who is vulnerable.",
      sql: C_MART, persona: "pipeline",
      after: async () => table(await lake.as("pipeline", (l) => l.runSQL(stripHead(C_MART)))) },
  ],
  martCount: async () => (await lake.as("pipeline", (l) => l.rows(`SELECT count(*) AS n FROM (${stripHead(C_MART)})`)))[0].n, martUnit: "outcome rows",
  files: { "01_sources.sql": C_SOURCES, "02_hubs.sql": C_HUBS, "03_link.sql": C_LINKS, "04_satellites.sql": C_SATS, "05_late_correction.sql": C_DAY2, "06_journey_outcomes.sql": C_MART },
  use: { title: "Transfers out and Consumer Duty: fair journeys", who: "Head of Consumer Duty · platform operations",
    q: "Do vulnerable clients wait longer to transfer out, and by how much?",
    lifepal: "Insurer turnaround (application to decision) was the same kind of measure. Feeds arrived up to 72 hours late; CDC with idempotent MERGE took reporting from 24 hours to under 5 minutes.",
    note: "Runs as the pipeline and returns aggregates only. The analyst sees the gap, never the flag.",
    persona: "pipeline", sql: stripHead(C_MART).replace("ORDER BY journey_type, client_group", "AND j.journey_type IN ('transfer_out', 'withdrawal')\nORDER BY journey_type, client_group").replace("GROUP BY ALL HAVING", "GROUP BY ALL HAVING"),
    say: (r) => { const t = r.filter((x) => x.journey_type === "transfer_out"), v = t.find((x) => x.client_group === "vulnerable"), o = t.find((x) => x.client_group !== "vulnerable");
      return v && o ? `Vulnerable clients take <b>${v.median_days} days</b> to transfer out against <b>${o.median_days}</b> for others (${(v.median_days / o.median_days).toFixed(1)}×). Above the 1.25× threshold, so the Duty owner hears it first, with this evidence.` : ""; } },
};

const VAULTS = { a: A, b: B, c: C };
let V = A, counts = {}, built = new Set(), usesReady = false;

// ---- the model diagram ------------------------------------------------------------------------
const W2 = { src: 65, sat: 70, hub: 62, link: 70, mart: 60 };
const edgePath = (a, b) => { const [x1, y1, , , k1] = V.N[a], [x2, y2, , , k2] = V.N[b];
  const sx = x1 + (x2 > x1 ? W2[k1] : x2 < x1 ? -W2[k1] : 0), ex = x2 + (x2 > x1 ? -W2[k2] : x2 < x1 ? W2[k2] : 0);
  const sy = x1 === x2 ? y1 + (y2 > y1 ? 23 : -23) : y1, ey = x1 === x2 ? y2 + (y2 > y1 ? -23 : 23) : y2, m = (sx + ex) / 2;
  return `M${sx},${sy} C${m},${sy} ${m},${ey} ${ex},${ey}`; };
function model(lit = new Set(), flows = []) {
  const fl = new Set(flows);
  const box = (id) => { const [x, y, label, sub, kind, tbl] = V.N[id], w = W2[kind] * 2;
    const c = tbl && counts[tbl] != null ? `${fmt(counts[tbl])} rows` : kind === "mart" && counts.mart != null ? `${fmt(counts.mart)} ${V.martUnit}` : sub;
    const state = lit.has(id) || lit.has(kind) ? "on" : built.has(id) ? "built" : kind === "src" ? "" : "todo";
    return `<g class="dvn ${kind} ${state}"><rect x="${x - w / 2}" y="${y - 23}" width="${w}" height="46" rx="8"/><text x="${x}" y="${y - 3}" class="t1">${label}</text><text x="${x}" y="${y + 12}" class="t2">${c}</text></g>`; };
  const edges = V.EDGES.map(([a, b]) => { const d = edgePath(a, b), on = fl.has(`${a}>${b}`);
    return `<path d="${d}" class="dve ${on ? "on" : ""}"/>${on && !still ? [0, 0.45, 0.9].map((t) => `<circle r="3.6" class="dvdot"><animateMotion dur="1.35s" begin="-${t}s" repeatCount="indefinite" path="${d}"/></circle>`).join("") : ""}`; }).join("");
  $("#model").innerHTML = `<svg viewBox="0 0 860 245" role="img" aria-label="Data Vault model: ${esc(V.title)}">${edges}${Object.keys(V.N).map(box).join("")}</svg>
    <div class="dv-legend"><span class="hub">Hub: one row per business key</span><span class="link">Link: a relationship</span><span class="sat">Satellite: a description, with history</span><span class="mart">Mart: what people query</span></div>`;
}
async function refreshCounts() {
  for (const t of Object.values(V.N).map((n) => n[5]).filter(Boolean)) {
    try { const [r] = await rows(`SELECT count(*) AS n FROM ${t}`); counts[t] = r.n; } catch { /* not built yet */ }
  }
}

// ---- steps --------------------------------------------------------------------------------------
async function runStep(i) {
  const s = V.steps[i], card = $(`#st-${i}`), out = $(".dv-out", card), btn = $("button", card);
  btn.disabled = true; out.innerHTML = '<p class="muted">Running…</p>';
  model(new Set(s.lit), V.FLOWS[s.key]);
  try {
    if (s.parallel) {
      out.innerHTML = `<div class="dv-par">${V.PAR.map((n) => `<div><small>${n}</small><i><em></em></i></div>`).join("")}</div>`;
      const ems = out.querySelectorAll("em"); ems.forEach((e, k) => { e.style.transition = `width ${0.9 + k * 0.25}s ease`; requestAnimationFrame(() => e.style.width = "100%"); });
      await sleep(1300);
    }
    if (s.query) out.innerHTML = table(await run(s.sql));
    else {
      if (s.persona) await lake.as(s.persona, (l) => l.runSQL(s.sql)); else await run(s.sql);
      out.innerHTML = (s.parallel ? out.innerHTML : "") + await s.after();
    }
    await refreshCounts();
    (V.BUILT[s.key] || []).forEach((k) => built.add(k));
    if (s.key === "mart") counts.mart = await V.martCount();
    model(new Set(), []);
    card.classList.add("done"); btn.textContent = "Done ✓";
    $("#prog").innerHTML = V.steps.map((_, k) => `<i class="${$(`#st-${k}`).classList.contains("done") ? "done" : ""}"></i>`).join("");
    if (i + 1 < V.steps.length) { $(`#st-${i + 1}`).hidden = false; } else finish();
  } catch (e) { out.innerHTML = `<div class="err">${esc(e.message)}</div>`; btn.disabled = false; }
}

function finish() {
  usesReady = true; renderUse();
  const el = document.createElement("div");
  el.className = "card"; el.innerHTML = `<span class="step">The judgement call</span><h2>Would you use a vault for every domain?</h2><div id="dc"></div>`;
  $("#dcwrap").replaceChildren(el);
  choose($("#dc"), [
    { t: "Yes: everything goes through the vault", gain: "One pattern everywhere.", cost: "Three extra tables for a single-source domain like HR, with nothing to integrate. Ceremony slows the team down." },
    { t: "No: Kimball straight from the sources, everywhere", gain: "Fewer tables, faster to build.", cost: "Integrating several businesses inside dimension builds, and losing what each system said and when." },
    { t: "Where several sources describe the same thing, or history must be auditable: vault in silver. Everywhere else: straight to Kimball", best: true,
      why: "Clients, advisers, firms and requests are described by several Quilter systems, and regulators can ask what we knew and when, so the vault earns its tables there. HR or marketing spend has one source, so it goes straight to a star. Either way gold looks the same to its users.",
      gain: "Integration and audit where they matter; simplicity elsewhere.", cost: "Two patterns to teach the team, so the decision rule is written down." },
  ]);
}

// ---- the use case this vault serves --------------------------------------------------------------
function renderUse() {
  const u = V.use;
  $("#uses").innerHTML = `<div class="card dv-uses"><span class="step">The use case this vault serves</span><div class="use ${usesReady ? "" : "locked"}" id="use-0"><h3>${esc(u.title)}</h3>
      <p class="use-q">${esc(u.q)}</p><p class="use-who">${esc(u.who)}</p>
      <p class="use-lp"><b>The Lifepal link.</b> ${esc(u.lifepal)}</p>
      <details><summary>The query</summary>${show(u.sql)}</details>
      <button class="small-btn solid" ${usesReady ? "" : "disabled"}>${usesReady ? "Run it on the vault" : "Build the vault first"}</button><div class="use-out"></div></div></div>`;
  $("#use-0 button").onclick = async () => {
    const out = $("#use-0 .use-out"); out.innerHTML = '<p class="muted">Running…</p>';
    try {
      const res = await lake.as(u.persona, (l) => l.runSQL(u.sql));
      const rs = res.rows.map((r) => Object.fromEntries(res.columns.map((k, j) => [k, r[j]])));
      out.innerHTML = `<p class="use-say">${u.say(rs)}</p>${table(res)}<p class="muted">${esc(u.note)}</p>`;
    } catch (e) { out.innerHTML = `<div class="err">${esc(e.message)}</div>`; }
  };
}

/** The SQL behind each vault, for the Live lakehouse code browser. */
export const VAULT_SQL = Object.fromEntries([["client_360", A], ["adviser_firms", B], ["transfers_duty", C]]
  .flatMap(([dir, v]) => Object.entries(v.files).map(([f, q]) => [`${dir}/${f}`, q])));

// ---- mount ------------------------------------------------------------------------------------------
function loadVault(key) {
  V = VAULTS[key]; counts = {}; built = new Set(); usesReady = false;
  document.querySelectorAll(".dv-switch button").forEach((b) => b.setAttribute("aria-pressed", b.dataset.v === key));
  $("#dvtitle").textContent = V.title; $("#dvintro").textContent = V.intro;
  $("#dvplay").disabled = false; $("#dvplay").textContent = "▶ Build it";
  $("#dvnow").innerHTML = "Press <b>Build it</b> to watch this vault being built on the live lakehouse, or run each step yourself below.";
  model();
  $("#steps").innerHTML = V.steps.map((s, i) => `<div class="card dv-step" id="st-${i}" ${i ? "hidden" : ""}><span class="step">${esc(s.sub)}</span><h2>${esc(s.title)}</h2><p>${esc(s.text)}</p>
    ${show(s.sql)}<button class="small-btn solid">${s.query ? "Ask the vault" : "Run it"}</button><div class="dv-out"></div></div>`).join("");
  V.steps.forEach((_, i) => $("button", $(`#st-${i}`)).onclick = () => runStep(i));
  $("#prog").innerHTML = V.steps.map(() => "<i></i>").join("");
  $("#dcwrap").innerHTML = "";
  renderUse();
}

/** Mount the three vaults into `root`; used by the chapter page and the Live lakehouse tab. */
export function mountVault(root, l) {
  lake = l;
  root.insertAdjacentHTML("beforeend", `<div class="dv-switch" role="group" aria-label="Choose a Data Vault">${Object.values(VAULTS).map((v) =>
      `<button data-v="${v.key}" aria-pressed="false"><b>${esc(v.button)}</b><small>${esc(v.title)}</small></button>`).join("")}</div>
    <div class="dv-model card"><div class="dv-head"><div><span class="step">The model</span><h2 id="dvtitle"></h2><p class="dv-intro" id="dvintro"></p></div>
    <button class="btn-run" id="dvplay">▶ Build it</button></div><p class="dv-now" id="dvnow"></p><div id="model"></div></div>
    <div class="progress" id="prog"></div><div id="steps"></div><div id="dcwrap"></div><div id="uses"></div>`);
  root.querySelectorAll(".dv-switch button").forEach((b) => b.onclick = () => { if (!$("#dvplay").disabled || $("#dvplay").textContent.startsWith("Built")) loadVault(b.dataset.v); });
  $("#dvplay").onclick = async () => {
    $("#dvplay").disabled = true;
    const stage = $("#model").closest(".dv-model");
    stage.classList.add("sticky");
    for (let i = 0; i < V.steps.length; i++) {
      const card = $(`#st-${i}`);
      if (card.classList.contains("done")) continue;
      card.hidden = false;
      card.scrollIntoView({ behavior: still ? "auto" : "smooth", block: "end" });
      $("#dvnow").innerHTML = `<b>${esc(V.steps[i].sub)}.</b> ${esc(V.steps[i].title)}`;
      model(new Set(V.steps[i].lit), V.FLOWS[V.steps[i].key]);
      await sleep(still ? 200 : 1700);
      await runStep(i);
      $("#dvnow").innerHTML = `<b>${esc(V.steps[i].sub)}.</b> ${esc(V.steps[i].title)} · <span class="ok">done</span>`;
      await sleep(still ? 200 : 900);
    }
    stage.classList.remove("sticky");
    $("#dvnow").innerHTML = `<b>Built.</b> ${esc(V.title)}: every table above is on the live lakehouse, and the use case below is unlocked.`;
    $("#dvplay").textContent = "Built ✓";
  };
  loadVault("a");
}

if (document.body.dataset.page === "vault") {
  header("vault.html");
  lakehouse($("#engine")).then((l) => {
    $("#engine").textContent = "Live lakehouse ready: every step runs on the real Delta tables in your browser.";
    mountVault($("#vault"), l);
  }).catch((e) => { $("#engine").textContent = `Could not start: ${e.message}`; });
}
