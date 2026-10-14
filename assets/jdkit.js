// Small helpers shared by the How I work chapters: the header, the lakehouse handle and an
// "evidence" block that runs real SQL against the Delta tables and shows the result.
import { openLakehouse } from "./engine.js";

export const $ = (s, r = document) => r.querySelector(s);
export const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
export const fmt = (n, d = 0) => Number(n).toLocaleString("en-GB", { maximumFractionDigits: d, minimumFractionDigits: d });

// The story: chapters in two halves (Lead, Serve), all on the same lakehouse.
export const PAGES = [
  { href: "design.html", label: "Design it", half: "Lead", n: 1, jd: "Technical leadership & domain ownership" },
  { href: "release.html", label: "Ship it", half: "Lead", n: 2, jd: "Engineering delivery" },
  { href: "review.html", label: "Grow the team", half: "Lead", n: 3, jd: "Mentoring & capability development" },
  { href: "simulate.html#people", label: "Work with the business", half: "Serve", n: 4, jd: "Cross-functional collaboration" },
  { href: "oncall.html", label: "Keep it running", half: "Serve", n: 5, jd: "Operational excellence" },
  { href: "duty.html", label: "Protect clients", half: "Serve", n: 6, jd: "Regulatory: Consumer Duty" },
  { href: "streaming.html", label: "Keep it live", half: "Serve", n: 7, jd: "Batch and real-time processing" },
];

export function header(current) {
  const el = document.createElement("header");
  el.className = "site-header";
  el.innerHTML = `<a class="brand" href="simulate.html"><span class="brand-mark" aria-hidden="true"></span><span><b>NEXUS</b> <span class="brand-for">How I work</span></span></a>
    <nav aria-label="How I work">${PAGES.map((p) => `<a href="${p.href}"${p.href === current ? ' aria-current="page" style="color:var(--mint)"' : ""} title="${esc(p.half)} · ${esc(p.jd)}">${p.n} ${esc(p.label)}</a>`).join("")}
    <a href="lakehouse.html" class="nav-code"><span class="live" aria-hidden="true"></span>Lakehouse</a></nav>`;
  document.body.prepend(el);
  const i = PAGES.findIndex((p) => p.href === current);
  if (i >= 0) {
    const crumb = document.querySelector(".jd-crumb");
    if (crumb) crumb.textContent = `How I work · ${PAGES[i].half} · Chapter ${PAGES[i].n}`;
    const prev = PAGES[i - 1], next = PAGES[i + 1];
    const nav = document.createElement("nav");
    nav.className = "story-nav"; nav.setAttribute("aria-label", "Chapters");
    nav.innerHTML = `${prev ? `<a href="${prev.href}"><small>← Chapter ${prev.n}</small>${esc(prev.label)}</a>` : `<a href="simulate.html"><small>← Start</small>How I work</a>`}
      ${next ? `<a class="next" href="${next.href}"><small>Chapter ${next.n} →</small>${esc(next.label)}</a>` : `<a class="next" href="lakehouse.html"><small>The foundation →</small>Open the lakehouse</a>`}`;
    document.querySelector("main").append(nav);
  }
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
