// Small helpers shared by the JD simulation pages: the header, the lakehouse handle and an
// "evidence" block that runs real SQL against the Delta tables and shows the result.
import { openLakehouse } from "./engine.js";

export const $ = (s, r = document) => r.querySelector(s);
export const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
export const fmt = (n, d = 0) => Number(n).toLocaleString("en-GB", { maximumFractionDigits: d, minimumFractionDigits: d });

export const PAGES = [
  { href: "design.html", label: "Design review", jd: "1 · Technical Leadership" },
  { href: "release.html", label: "Requirement to release", jd: "2 · Engineering Delivery" },
  { href: "review.html", label: "Review a PR", jd: "3 · Mentoring" },
  { href: "simulate.html#people", label: "Stakeholder week", jd: "4 · Cross-Functional" },
  { href: "oncall.html", label: "On-call shift", jd: "5 · Operational Excellence" },
  { href: "duty.html", label: "Consumer Duty", jd: "Regulatory" },
  { href: "streaming.html", label: "Streaming", jd: "Skills · real-time" },
];

export function header(current) {
  const el = document.createElement("header");
  el.className = "site-header";
  el.innerHTML = `<a class="brand" href="simulate.html"><span class="brand-mark" aria-hidden="true"></span><span><b>NEXUS</b> <span class="brand-for">Simulations</span></span></a>
    <nav aria-label="Simulations">${PAGES.map((p) => `<a href="${p.href}"${p.href === current ? ' aria-current="page" style="color:var(--mint)"' : ""} title="${esc(p.jd)}">${esc(p.label)}</a>`).join("")}
    <a href="lakehouse.html" class="nav-code">Lakehouse ↗</a></nav>`;
  document.body.prepend(el);
}

let lake;
export async function lakehouse(statusEl) {
  lake ??= await openLakehouse({ status: (s) => { if (statusEl) statusEl.textContent = s; } });
  return lake;
}

/** Render a SQL "evidence" block: shows the query, runs it on the lakehouse, shows the rows. */
export async function evidence(el, title, sqlText, persona) {
  el.innerHTML = `<div class="ev"><div class="ev-bar"><span><b>Evidence from the lakehouse</b> · ${esc(title)}</span><button class="small-btn">Run</button></div>
    <pre>${esc(sqlText.trim())}</pre><div class="res"></div></div>`;
  const out = $(".res", el), btn = $("button", el);
  const run = async () => {
    btn.disabled = true; out.innerHTML = '<div class="err" style="color:#56665c">Running…</div>';
    try {
      const l = await lakehouse();
      const res = persona ? await l.as(persona, (x) => x.runSQL(sqlText)) : await l.runSQL(sqlText);
      out.innerHTML = table(res);
      return res;
    } catch (e) { out.innerHTML = `<div class="err">${esc(e.message)}</div>`; return null; }
    finally { btn.disabled = false; }
  };
  btn.onclick = run;
  return run;
}

export function table(res) {
  return `<table><tr>${res.columns.map((c) => `<th>${esc(c)}</th>`).join("")}</tr>${res.rows.map((r) =>
    `<tr>${r.map((v) => `<td>${v == null ? '<span style="color:#9aa89f">null</span>' : esc(typeof v === "number" && !Number.isInteger(v) ? +v.toFixed(4) : v)}</td>`).join("")}</tr>`).join("")}</table>`;
}

/** Options with feedback. opts: [{t, sub, best, why, gain, cost}] → resolves with the picked option. */
export function choose(el, opts, { onPick } = {}) {
  el.innerHTML = `<div class="opts">${opts.map((o, i) => `<button data-i="${i}">${esc(o.t)}${o.sub ? `<small>${esc(o.sub)}</small>` : ""}</button>`).join("")}</div><div class="fbx"></div>`;
  el.querySelectorAll(".opts button").forEach((b) => b.onclick = () => {
    const o = opts[+b.dataset.i], best = opts.find((x) => x.best);
    el.querySelectorAll(".opts button").forEach((x) => {
      x.disabled = true;
      if (x === b) x.classList.add("picked");
      if (opts[+x.dataset.i].best) x.classList.add("best");
    });
    $(".fbx", el).innerHTML = `<div class="fb">${o.best ? "<b>That's what I'd choose.</b> " : `<b>I'd go another way:</b> ${esc(best.t)}. `}${esc(o.best ? o.why : best.why)}
      ${o.gain || o.cost ? `<div class="tradeoff">${o.gain ? `<div class="gain"><b>You gain</b><br>${esc(o.gain)}</div>` : ""}${o.cost ? `<div class="cost"><b>You pay</b><br>${esc(o.cost)}</div>` : ""}</div>` : ""}</div>`;
    onPick?.(o);
  });
}

export function download(name, text, type = "text/markdown") {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([text], { type }));
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}
