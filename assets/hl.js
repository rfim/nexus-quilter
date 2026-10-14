// Syntax colouring for every code block on the site: SQL, Python, YAML and diffs.
// Classic script (sets globalThis.NexusHL) so both site.js and the ES modules can use it.
(function (g) {
  const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
  const span = (cls, t) => `<span class="hl-${cls}">${esc(t)}</span>`;
  const set = (s) => new Set(s.split(/\s+/).filter(Boolean));

  const SQL_KW = set(`SELECT FROM WHERE AND OR NOT IN IS AS ON JOIN LEFT RIGHT INNER OUTER FULL CROSS USING GROUP BY ORDER
    HAVING QUALIFY LIMIT OFFSET UNION ALL DISTINCT WITH RECURSIVE CASE WHEN THEN ELSE END CREATE REPLACE OR TABLE VIEW
    FUNCTION RETURN RETURNS ALTER COLUMN SET TAGS MASK ROW FILTER INSERT INTO VALUES UPDATE DELETE MERGE MATCHED
    CONFLICT DO EXCLUDED GRANT REVOKE TO OWNER COMMENT INTERVAL EXISTS BETWEEN LIKE ILIKE OVER PARTITION ROWS RANGE
    PRECEDING FOLLOWING CURRENT UNBOUNDED WINDOW FILTER DESC ASC VERSION TIMESTAMP OF DESCRIBE DETAIL HISTORY SHOW
    GRANTS TABLES SCHEMA CATALOG TBLPROPERTIES CLUSTER LANGUAGE APPLY PURGE VACUUM OPTIMIZE ZORDER EXECUTE ANY
    PRIMARY KEY DEFAULT DROP IF CAST TRY_CAST ANTI SEMI LATERAL UNNEST`);
  const SQL_TYPES = set(`BIGINT INT INTEGER SMALLINT TINYINT STRING VARCHAR CHAR TEXT DECIMAL NUMERIC DOUBLE FLOAT REAL
    BOOLEAN DATE TIMESTAMP TIMESTAMP_NTZ BINARY ARRAY MAP STRUCT`);
  const SQL_CONST = set("NULL TRUE FALSE");

  const PY_CTL = set("if elif else for while return import from as with try except finally raise yield break continue pass in and or not is assert await async del global nonlocal");
  const PY_KW = set("def class lambda");
  const PY_CONST = set("None True False self cls");
  const PY_BUILTIN = set("print len range list dict set tuple str int float bool sum min max abs round sorted enumerate zip map filter open isinstance super any all");

  function sql(src) {
    const re = /(--[^\n]*|\/\*[\s\S]*?\*\/)|('(?:[^'\n]|'')*')|(`[^`\n]*`)|(\b\d+(?:\.\d+)?\b)|(\bnexus\.\w+\.\w+\b)|([A-Za-z_][\w]*)(?=\s*\()|([A-Za-z_][\w]*)|(<>|!=|>=|<=|\|\||::|[=<>+\-*\/%])/g;
    let out = "", last = 0;
    src.replace(re, (t, com, str, bq, num, tbl, fn, word, op, off) => {
      out += esc(src.slice(last, off)); last = off + t.length;
      const up = (fn || word || "").toUpperCase();
      if (com) out += span("com", t);
      else if (str) out += span("str", t);
      else if (bq) out += span("var", t);
      else if (num) out += span("num", t);
      else if (tbl) out += span("type", t);
      else if (fn) out += SQL_KW.has(up) && !/^(IF|COALESCE)$/.test(up) && up !== "REPLACE" ? span("kw", t) : span("fn", t);
      else if (word) out += SQL_CONST.has(up) ? span("const", t) : SQL_TYPES.has(up) ? span("type", t) : SQL_KW.has(up) ? span("kw", t) : esc(t);
      else out += span("op", t);
      return t;
    });
    return out + esc(src.slice(last));
  }

  function python(src) {
    const re = /(#[^\n]*)|((?:[rbfRBF]{1,2})?(?:"""[\s\S]*?"""|'''[\s\S]*?'''|"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)*'))|(@[\w.]+)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_]\w*)|([=+\-*\/%<>!]=?|\*\*|->)/g;
    let out = "", last = 0, prev = "";
    src.replace(re, (t, com, str, dec, num, word, op, off) => {
      out += esc(src.slice(last, off));
      const after = src.slice(off + t.length);
      last = off + t.length;
      if (com) out += span("com", t);
      else if (str) out += span("str", t);
      else if (dec) out += span("fn", t);
      else if (num) out += span("num", t);
      else if (word) {
        if (prev === "def") out += span("fn", t);
        else if (prev === "class") out += span("type", t);
        else if (PY_KW.has(t)) out += span("kw", t);
        else if (PY_CTL.has(t)) out += span("ctl", t);
        else if (PY_CONST.has(t)) out += span("const", t);
        else if (/^\s*\(/.test(after)) out += span("fn", t);
        else if (/^[A-Z][a-z]\w*$/.test(t) || PY_BUILTIN.has(t)) out += span("type", t);
        else if (/^\s*=[^=]/.test(after) && /[(,]\s*$/.test(src.slice(0, off))) out += span("var", t);
        else out += esc(t);
        prev = t;
        return t;
      } else out += span("op", t);
      prev = "";
      return t;
    });
    return out + esc(src.slice(last));
  }

  function yamlValue(v) {
    const re = /(\s+#[^\n]*$)|("(?:[^"\\]|\\.)*"|'[^']*')|([\w.@$\/-]+)(?=\s*:\s)|(\b(?:true|false|null|yes|no)\b)|(-?\b\d+(?:\.\d+)?%?(?![\w.]))|([\[\]{},:])|([^\s\[\]{},#"':]+(?:\s+[^\s\[\]{},#"':]+)*)/g;
    let out = "", last = 0;
    v.replace(re, (t, com, str, key, bool, num, punc, plain, off) => {
      out += esc(v.slice(last, off)); last = off + t.length;
      out += com ? span("com", t) : str ? span("str", t) : key ? span("key", t) : bool ? span("const", t) : num ? span("num", t) : punc ? span("op", t) : span("str", t);
      return t;
    });
    return out + esc(v.slice(last));
  }
  function yaml(src) {
    return src.split("\n").map((line) => {
      if (/^\s*#/.test(line)) return span("com", line);
      const m = line.match(/^(\s*)(-\s+)?([\w.@$\/-]+)(\s*:)(.*)$/);
      if (m) return esc(m[1]) + (m[2] ? span("op", m[2]) : "") + span("key", m[3]) + span("op", m[4]) + yamlValue(m[5]);
      const li = line.match(/^(\s*)(-\s+)(.*)$/);
      if (li) return esc(li[1]) + span("op", li[2]) + yamlValue(li[3]);
      return yamlValue(line);
    }).join("\n");
  }

  // unified diff of any language: + and - lines tinted, the rest coloured as `lang`
  function diff(src, lang = "yaml") {
    return src.split("\n").map((line) => {
      const m = line.match(/^([+-])(.*)$/);
      const body = hl(m ? " " + m[2] : line, lang);
      return m ? `<span class="hl-${m[1] === "+" ? "add" : "del"}">${esc(m[1])}${body.replace(/^ /, "")}</span>` : body;
    }).join("\n");
  }

  function hl(src, lang) {
    src = String(src ?? "");
    if (lang === "sql") return sql(src);
    if (lang === "python" || lang === "py") return python(src);
    if (lang === "yaml" || lang === "yml") return yaml(src);
    if (lang && lang.startsWith("diff")) return diff(src, lang.split(":")[1] || "yaml");
    return esc(src);
  }
  g.NexusHL = { hl, esc };
})(globalThis);
