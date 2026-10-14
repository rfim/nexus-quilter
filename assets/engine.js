// NEXUS lakehouse engine, shared by the lakehouse page and every simulation.
//
// Delta: each table's _delta_log commits (newline-delimited JSON actions) are replayed to get the
// exact set of Parquet files at any version, so DESCRIBE HISTORY / DETAIL and VERSION AS OF /
// TIMESTAMP AS OF come straight from the log. Query: parquet-wasm (Rust arrow-rs) decodes each
// data file and DuckDB-WASM runs the SQL over the Arrow data; no DuckDB extension is downloaded.
// Unity Catalog: three-level names, grants, column masks and row filters by persona. It simulates
// the behaviour; it is not Databricks itself.
const DUCKDB = "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";
const PARQUET = "https://cdn.jsdelivr.net/npm/parquet-wasm@0.6.1/esm/parquet_wasm.js";
const PARQUET_WASM = "https://cdn.jsdelivr.net/npm/parquet-wasm@0.6.1/esm/parquet_wasm_bg.wasm";
const BASE = new URL("assets/lakehouse/", document.baseURI);
const MAX_ROWS = 500;

// ---- personas: who you are decides grants, masks and row filters ------------------------------
export const PERSONAS = [
  { key: "engineer", label: "VIM · data platform engineer", groups: ["grp_platform"], mask: "hash" },
  { key: "dpo", label: "Data protection officer", groups: ["grp_dpo"], mask: "none" },
  { key: "finance", label: "Finance analyst", groups: ["grp_finance"], mask: "redact" },
  { key: "exec", label: "Executive MI (all entities)", groups: ["grp_exec_mi"], mask: "redact" },
  { key: "affluent", label: "Executive MI (Affluent only)", groups: ["grp_exec_mi"], mask: "redact", rowFilter: "affluent" },
  { key: "marketing", label: "Marketing analyst", groups: ["grp_marketing"], mask: "redact" },
  { key: "hr", label: "People analytics (HR)", groups: ["grp_hr"], mask: "redact" },
  { key: "duty", label: "Consumer Duty analyst", groups: ["grp_consumer_duty"], mask: "redact" },
  { key: "pipeline", label: "nexus-daily job (service principal)", groups: ["grp_platform"], mask: "none" },
];
let persona = PERSONAS[0];

function canRead(t, p = persona) {
  const g = new Set(p.groups);
  if (t.derived) return g.has("grp_platform") || t.read.some((r) => g.has(r));
  if (t.layer === "bronze") return g.has("grp_platform");
  if (t.layer === "silver") return g.has("grp_platform") || g.has("grp_dpo");
  if (t.name === "nexus.gold.fct_platform_transaction") return g.has("grp_platform") || g.has("grp_finance") || g.has("grp_dpo");
  return g.has("grp_platform") || g.has("grp_dpo") || t.read.some((r) => g.has(r));
}
function grantsFor(t) {
  if (t.derived) return ["grp_platform", ...t.read];
  if (t.layer === "bronze") return ["grp_platform"];
  if (t.layer === "silver") return ["grp_platform", "grp_dpo"];
  if (t.name === "nexus.gold.fct_platform_transaction") return ["grp_platform", "grp_finance", "grp_dpo"];
  return ["grp_platform", "grp_dpo", ...t.read];
}

// ---- Delta log replay -----------------------------------------------------------------------
const TABLES = new Map();      // "nexus.gold.x" -> table
const DERIVED = new Map();     // tables published in the browser by a simulation (see api.publish)
const loaded = new Map();       // data file path -> DuckDB table holding its rows
let pq;

async function loadTable(t) {
  t.commits = [];
  for (let v = 0; v < t.versions; v++) {
    const text = await (await fetch(new URL(`${t.path}/_delta_log/${String(v).padStart(20, "0")}.json`, BASE))).text();
    t.commits.push({ version: v, actions: text.trim().split("\n").map((l) => JSON.parse(l)) });
  }
  t.current = t.versions - 1;
  return t;
}

function snapshot(t, version = t.current) {
  if (version < 0 || version > t.current) throw new Error(`[DELTA_VERSIONS_NOT_CONTIGUOUS] Cannot time travel ${t.name} to version ${version}. Available versions: [0, ${t.current}].`);
  const files = new Map();
  let meta = null, info = null;
  for (const c of t.commits.slice(0, version + 1)) {
    for (const a of c.actions) {
      if (a.metaData) meta = a.metaData;
      if (a.add) files.set(a.add.path, a.add);
      if (a.remove) files.delete(a.remove.path);
      if (a.commitInfo) info = a.commitInfo;
    }
  }
  const fields = JSON.parse(meta.schemaString).fields;
  return { version, files: [...files.values()], fields, info };
}

function versionAt(t, ts) {
  const when = Date.parse(ts);
  let found = -1;
  for (const c of t.commits) {
    const info = c.actions.find((a) => a.commitInfo)?.commitInfo;
    if (info && info.timestamp <= when) found = c.version;
  }
  if (found < 0) throw new Error(`[DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION] ${ts} is before the first commit of ${t.name}.`);
  return found;
}

const DUCK_TYPES = { string: "VARCHAR", double: "DOUBLE", long: "BIGINT", integer: "INTEGER", date: "DATE",
  timestamp: "TIMESTAMP", boolean: "BOOLEAN", float: "FLOAT", short: "SMALLINT", byte: "TINYINT" };
function duckType(type) {
  if (typeof type === "object" && type.type === "array") return `${duckType(type.elementType)}[]`;
  const m = String(type).match(/^decimal\((\d+),\s*(\d+)\)$/);
  return m ? `DECIMAL(${m[1]},${m[2]})` : DUCK_TYPES[type] || "VARCHAR";
}
function deltaType(type) { return typeof type === "object" ? `array<${type.elementType}>` : type; }

async function loadFile(name) {
  if (!loaded.has(name)) {
    const bytes = new Uint8Array(await (await fetch(new URL(name, BASE))).arrayBuffer());
    const table = `f${loaded.size}`;
    await conn.insertArrowFromIPCStream(pq.readParquet(bytes).intoIPCStream(), { name: table, schema: "main", create: true });
    loaded.set(name, table);
  }
  return loaded.get(name);
}

// A version is the union of the data files its log says are active
async function scan(t, version) {
  const snap = snapshot(t, version);
  if (!snap.files.length) {
    return `(SELECT ${snap.fields.map((f) => `NULL::${duckType(f.type)} AS "${f.name}"`).join(", ")} WHERE false)`;
  }
  const parts = [];
  for (const f of snap.files) parts.push(`SELECT * FROM main.${await loadFile(`${t.path}/${f.path}`)}`);
  return `(${parts.join(" UNION ALL BY NAME ")})`;
}

// Unity Catalog: column masks and row filters wrap the scan
async function governed(t, version) {
  const snap = snapshot(t, version);
  const src = await scan(t, version);
  const pii = Object.keys(t.pii || {});
  const replace = pii.filter(() => persona.mask !== "none").map((c) => {
    const f = snap.fields.find((x) => x.name === c);
    if (!f) return null;
    if (f.type === "boolean") return `NULL::BOOLEAN AS "${c}"`;
    return persona.mask === "hash" ? `sha256(lower(trim("${c}"))) AS "${c}"` : `'***' AS "${c}"`;
  }).filter(Boolean);
  const hasEntity = snap.fields.some((f) => f.name === "legal_entity");
  const where = persona.rowFilter && hasEntity ? ` WHERE legal_entity = '${persona.rowFilter}'` : "";
  return `(SELECT *${replace.length ? ` REPLACE (${replace.join(", ")})` : ""} FROM ${src}${where})`;
}

// ---- DuckDB ---------------------------------------------------------------------------------
let duck, db, conn;
async function boot() {
  setEngine("Loading DuckDB-WASM and parquet-wasm…");
  pq = await import(PARQUET);
  await pq.default(PARQUET_WASM);
  duck = await import(DUCKDB);
  const bundle = await duck.selectBundle(duck.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
  db = new duck.AsyncDuckDB(new duck.VoidLogger(), new Worker(workerUrl));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  await db.open({ query: { castBigIntToDouble: true, castDecimalToDouble: true } });
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();
  await conn.query("ATTACH ':memory:' AS nexus");
  for (const s of ["bronze", "silver", "gold"]) await conn.query(`CREATE SCHEMA IF NOT EXISTS nexus.${s}`);

  setEngine("Replaying Delta logs…");
  const cat = await (await fetch(new URL("catalog.json", BASE))).json();
  await Promise.all(cat.tables.map(async (t) => TABLES.set(t.name, await loadTable(t))));
  await refreshViews();
  const v = (await conn.query("SELECT version() AS v")).toArray()[0].v;
  setEngine(`DuckDB ${v} · ${TABLES.size} Delta tables`);
}

async function refreshViews() {
  for (const t of TABLES.values()) {
    await conn.query(`CREATE OR REPLACE VIEW ${t.name} AS SELECT * FROM ${await governed(t, t.current)}`);
  }
}

let setEngine = () => {};

// ---- SQL: Databricks / Delta syntax on top of DuckDB ----------------------------------------
const REF = /\bnexus\.(bronze|silver|gold)\.(\w+)(?:\s+(?:VERSION\s+AS\s+OF\s+(\d+)|TIMESTAMP\s+AS\s+OF\s+'([^']+)')|@v(\d+))?/gi;

function table(name) {
  const t = TABLES.get(name.toLowerCase()) ?? DERIVED.get(name.toLowerCase());
  if (!t) throw new Error(`[TABLE_OR_VIEW_NOT_FOUND] The table or view \`${name}\` cannot be found.`);
  return t;
}
function checkGrant(t) {
  if (!canRead(t)) throw new Error(`[INSUFFICIENT_PERMISSIONS] Insufficient privileges:\nUser '${persona.label}' (${persona.groups.join(", ")}) does not have SELECT on Table '${t.name}'.\nGranted to: ${grantsFor(t).join(", ")}`);
}

async function special(sql) {
  let m;
  if ((m = sql.match(/^DESCRIBE\s+(?:HISTORY|DETAIL)\s+([\w.]+)$/i)) && table(m[1]).derived) {
    const t = table(m[1]);
    return { columns: ["name", "createdBy", "readers", "comment"], rows: [[t.name, t.createdBy, grantsFor(t).join(", "), t.comment ?? null]] };
  }
  if ((m = sql.match(/^DESCRIBE\s+HISTORY\s+([\w.]+)$/i))) {
    const t = table(m[1]);
    return { columns: ["version", "timestamp", "operation", "summary", "operationMetrics", "engineInfo"],
      rows: [...t.commits].reverse().map((c) => {
        const i = c.actions.find((a) => a.commitInfo).commitInfo, mm = i.operationMetrics || {};
        const summary = i.operation === "MERGE"
          ? `inserted ${mm.numTargetRowsInserted ?? 0} · updated ${mm.numTargetRowsUpdated ?? 0} · deleted ${mm.numTargetRowsDeleted ?? 0} · source rows ${mm.numSourceRows ?? "?"}`
          : `rows written ${mm.numOutputRows ?? "?"} · files ${mm.numFiles ?? "?"}`;
        return [c.version, new Date(i.timestamp).toISOString().replace("T", " ").slice(0, 19), i.operation,
          summary, JSON.stringify(mm), i.engineInfo];
      }) };
  }
  if ((m = sql.match(/^DESCRIBE\s+DETAIL\s+([\w.]+)$/i))) {
    const t = table(m[1]), s = snapshot(t);
    return { columns: ["format", "name", "location", "currentVersion", "numFiles", "sizeInBytes", "owner", "comment"],
      rows: [["delta", t.name, `assets/lakehouse/${t.path}`, t.current, s.files.length,
        s.files.reduce((a, f) => a + f.size, 0), t.owner, t.comment]] };
  }
  if ((m = sql.match(/^DESCRIBE\s+(?:TABLE\s+)?([\w.]+)$/i))) {
    const t = table(m[1]);
    return { columns: ["col_name", "data_type", "tag"], rows: snapshot(t).fields.map((f) => [f.name, deltaType(f.type), t.pii?.[f.name] ? `pii: ${t.pii[f.name]}` : null]) };
  }
  if ((m = sql.match(/^SHOW\s+TABLES\s+IN\s+nexus\.(\w+)$/i))) {
    return { columns: ["database", "tableName", "versions", "canSelect"],
      rows: [...TABLES.values()].filter((t) => t.layer === m[1].toLowerCase()).map((t) => [`nexus.${t.layer}`, t.name.split(".")[2], t.versions, canRead(t)]) };
  }
  if ((m = sql.match(/^SHOW\s+GRANTS\s+ON\s+(?:TABLE\s+)?([\w.]+)$/i))) {
    const t = table(m[1]);
    return { columns: ["principal", "actionType", "objectType", "objectKey"], rows: grantsFor(t).map((g) => [g, "SELECT", "TABLE", t.name]) };
  }
  if (/^SELECT\s+current_user\(\)$/i.test(sql)) return { columns: ["current_user()"], rows: [[persona.label]] };
  return null;
}

async function rewrite(sql) {
  const parts = [];
  let last = 0;
  for (const m of sql.matchAll(REF)) {
    const t = table(`nexus.${m[1]}.${m[2]}`);
    checkGrant(t);
    parts.push(sql.slice(last, m.index));
    const v = m[3] ?? m[5];
    if (t.derived) {
      if (v != null || m[4]) throw new Error(`[DELTA_UNSUPPORTED_TIME_TRAVEL] ${t.name} was published in this session and has no history yet.`);
      parts.push(`main.${t.store}`);
    } else if (v != null) parts.push(await governed(t, Number(v)));
    else if (m[4]) parts.push(await governed(t, versionAt(t, m[4])));
    else parts.push(m[0]);
    last = m.index + m[0].length;
  }
  return parts.join("") + sql.slice(last);
}

async function runSQL(text) {
  const statements = text.split(";").map((s) => s.replace(/--[^\n]*/g, "").trim()).filter(Boolean);
  let result = null;
  const t0 = performance.now();
  for (const s of statements) {
    result = (await special(s)) ?? arrowRows(await conn.query(await rewrite(s)));
  }
  return { ...result, ms: Math.round(performance.now() - t0) };
}

function arrowRows(tbl) {
  const fields = tbl.schema.fields;
  const fmt = fields.map((f) => {
    const ty = String(f.type);
    if (/^Date/.test(ty)) return (v) => v == null ? null : new Date(v).toISOString().slice(0, 10);
    if (/^Timestamp/.test(ty)) {
      const div = [1e-3, 1, 1e3, 1e6][f.type.unit] ?? 1;   // seconds, ms, us, ns -> ms
      return (v) => v == null ? null : new Date(Number(v) / div).toISOString().replace("T", " ").slice(0, 19);
    }
    return (v) => v == null ? null : (typeof v === "object" && v.toJSON ? JSON.stringify(v.toJSON()) : v);
  });
  const rows = [];
  for (const r of tbl) {
    if (rows.length >= MAX_ROWS) break;
    const o = r.toJSON();
    rows.push(fields.map((f, i) => fmt[i](o[f.name])));
  }
  return { columns: fields.map((f) => f.name), rows, total: tbl.numRows };
}

/** Boot once and share: every page that imports this gets the same lakehouse. */
let opening;
export function openLakehouse({ status } = {}) {
  if (status) setEngine = status;
  opening ??= boot().then(() => api);
  return opening;
}

const api = {
  PERSONAS,
  tables: TABLES,
  get persona() { return persona; },
  async setPersona(key) {
    persona = PERSONAS.find((p) => p.key === key) ?? PERSONAS[0];
    await refreshViews();
    return persona;
  },
  canRead: (t, p) => canRead(t, p ?? persona),
  grantsFor,
  snapshot,
  runSQL,
  /** Run SQL and return plain row objects (for charts and logic, not display). */
  async rows(sqlText) {
    const res = await runSQL(sqlText);
    return res.rows.map((r) => Object.fromEntries(res.columns.map((c, i) => [c, r[i]])));
  },
  /** Run SQL as a given persona without changing the page's persona. */
  async as(key, fn) {
    const before = persona.key;
    await api.setPersona(key);
    try { return await fn(api); } finally { await api.setPersona(before); }
  },
  /** Create a table from a query, as the current persona, readable by the given groups. */
  async publish(name, sqlText, { read = [], comment } = {}) {
    const store = `pub_${name.split(".").pop()}`;
    const sql = await rewrite(sqlText.replace(/--[^\n]*/g, "").trim().replace(/;$/, ""));
    await conn.query(`CREATE OR REPLACE TABLE main.${store} AS ${sql}`);
    DERIVED.set(name, { name, layer: name.split(".")[1], read, store, derived: true, comment, createdBy: persona.label });
  },
  get conn() { return conn; },
};
