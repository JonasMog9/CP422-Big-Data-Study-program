/* CP422 Midterm Lab: app logic. Vanilla JS, no build step. Content lives in content-*.js. */
(function () {
"use strict";

const CP = window.CP;
const TOPIC = Object.fromEntries(CP.topics.map(t => [t.id, t]));
const TRACK = Object.fromEntries(CP.tracks.map(t => [t.id, t]));
const CODE = Object.fromEntries(CP.code.map(c => [c.id, c]));
const SHORT = Object.fromEntries(CP.short.map(s => [s.id, s]));
const QUESTIONS = [
  ...CP.mcq.map(q => ({ ...q, kind: "mcq" })),
  ...CP.tf.map(q => ({ ...q, kind: "tf" }))
];
const QBYID = Object.fromEntries(QUESTIONS.map(q => [q.id, q]));
const VIEWS = ["plan", "notes", "quiz", "code", "short", "mock"];

/* ---------------- helpers ---------------- */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[c]));
const md = s => esc(s)
  .replace(/`([^`]+)`/g, "<code>$1</code>")
  .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
const shuffle = arr => { const a = arr.slice(); for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const pct = (a, b) => b ? Math.round(100 * a / b) : 0;
const LETTERS = "ABCDEFGH";

function hl(code, lang) {
  const language = { python: "python", java: "java", sql: "sql", bash: "bash" }[lang];
  if (window.hljs && language) {
    try { return window.hljs.highlight(code, { language, ignoreIllegals: true }).value; } catch (e) { /* plain */ }
  }
  return esc(code);
}
const codeBlock = (code, lang, cls = "") => `<pre class="code ${cls}"><code>${hl(code, lang)}</code></pre>`;

/* ---------------- storage (per-viewer progress, best effort) ---------------- */
const KEY = "cp422-midterm-lab-v1";
let S = {};
try { S = JSON.parse(localStorage.getItem(KEY)) || {}; } catch (e) { S = {}; }
S.q = S.q || {};       // question id -> {n, c, last}
S.code = S.code || {}; // exercise id -> {done, score, draft}
S.sa = S.sa || {};     // short answer id -> {draft, score}
S.plan = S.plan || {};
S.mocks = S.mocks || [];
let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => { try { localStorage.setItem(KEY, JSON.stringify(S)); } catch (e) { /* storage unavailable */ } }, 150);
}
function record(id, ok) {
  const r = S.q[id] || (S.q[id] = { n: 0, c: 0 });
  r.n++; if (ok) r.c++; r.last = ok ? 1 : 0;
  save();
}
const mistakes = () => QUESTIONS.filter(q => S.q[q.id] && S.q[q.id].last === 0);
const codeState = id => S.code[id] || (S.code[id] = {});

/* ---------------- Claude (sample capability) ---------------- */
let AI = null;
const aiPill = $("#ai-pill");
function setAI(fn) {
  AI = fn;
  document.body.classList.toggle("ai-on", !!AI);
  aiPill.classList.toggle("ai-ready", !!AI);
  aiPill.innerHTML = AI ? '<span class="dot"></span>Claude grading on' : '<span class="dot"></span>Self-check mode';
  aiPill.title = AI ? "Claude can grade your code and short answers (uses your Claude usage)." : "Claude grading isn't available here. Use the model answers and rubrics to check yourself.";
}
(async () => {
  let fn = null;
  try { if (window.claude && typeof window.claude.use === "function") fn = await window.claude.use("sample"); } catch (e) { fn = null; }
  setAI(fn);
})();
function aiErrorText(e) {
  const code = e && e.code;
  if (code === "not_granted" || code === "sampling_disabled" || code === "not_declared" || code === "capability_disabled" || code === "capability_removed") {
    setAI(null);
    return "Claude isn't allowed on this page right now, so grading is off. Use the model answer and rubric below.";
  }
  if (code === "rate_limited") return "Too many requests for now. Wait a minute and try again.";
  if (code === "session_expired") return "Your Claude session expired. Sign in again, then retry.";
  if (code === "prompt_too_large") return "Your answer is too long to send. Trim it and try again.";
  if (code === "invalid_json") return "Claude's reply couldn't be read. Try again.";
  if (code === "cancelled") return "";
  return "Something went wrong reaching Claude. Try again in a moment.";
}

const SLIDE_RULES = CP.slideStyle.map(t => `${t.name}:\n- ` + t.items.map(i => i.replace(/`/g, "")).join("\n- ")).join("\n");
const GRADER_INTRO = `You are a fair, encouraging TA grading a practice answer for the CP422 "Programming for Big Data" midterm (Wilfrid Laurier University).
The professor's marking rules:
- Code is marked on LOGIC and completeness, not syntax. Ignore missing semicolons, brackets, imports, small typos and capitalization.
- He only accepts the approach taught in the lecture slides and labs ("his way"). If the student uses a method that is not one of the slide patterns below, still credit correct logic but point out the slide-style equivalent in "offSlide".
- Short answers are about comparing, describing differences and explaining why.
Slide patterns taught in the course:
${SLIDE_RULES}`;

async function gradeWithClaude(task, answer, box, btn) {
  if (!AI) return;
  if (!answer.trim()) { box.hidden = false; box.innerHTML = '<div class="note warn">Write an answer first, then ask Claude to grade it.</div>'; return; }
  const rubricText = task.rubric.map((r, i) => `${i + 1}. (${r.pts} marks) ${r.text}`).join("\n");
  const outOf = task.rubric.reduce((a, r) => a + r.pts, 0);
  const prompt = `${GRADER_INTRO}

QUESTION (${task.langName}):
${task.question}

RUBRIC (total ${outOf} marks):
${rubricText}

REFERENCE ANSWER (one correct way; any logically equivalent answer in the slide style earns full marks):
${task.reference}

STUDENT ANSWER:
${answer}

Grade the student answer against the rubric. Reply with only a JSON object, no other text:
{"score": number, "outOf": ${outOf}, "verdict": "one sentence", "rubric": [{"item": "short rubric item", "earned": number, "max": number, "note": "short reason"}], "fixes": ["specific, actionable fix"], "offSlide": ["construct not from the slides -> the slide-style way"], "nextStep": "one concrete thing to practise next"}`;
  box.hidden = false;
  box.innerHTML = '<div class="ai-box"><p><strong>Claude is grading…</strong> <span class="muted small">This can take up to a minute.</span></p></div>';
  if (btn) btn.disabled = true;
  try {
    const r = await AI.json(prompt, { modelTier: "default" });
    const score = Number(r.score) || 0;
    const rows = Array.isArray(r.rubric) ? r.rubric : [];
    box.innerHTML = `<div class="ai-box stack fade-in">
      <div class="row"><div class="score">${esc(score)}/${esc(r.outOf ?? outOf)}</div><p>${md(r.verdict || "")}</p></div>
      ${rows.length ? `<div class="table-wrap"><table><thead><tr><th>Rubric item</th><th>Marks</th><th>Why</th></tr></thead><tbody>${rows.map(x => `<tr><td>${md(x.item)}</td><td>${esc(x.earned)}/${esc(x.max)}</td><td>${md(x.note || "")}</td></tr>`).join("")}</tbody></table></div>` : ""}
      ${r.fixes && r.fixes.length ? `<div><h4>Fix these</h4><ul>${r.fixes.map(f => `<li>${md(f)}</li>`).join("")}</ul></div>` : ""}
      ${r.offSlide && r.offSlide.length ? `<div class="note warn"><strong>Not the slide way:</strong><ul>${r.offSlide.map(f => `<li>${md(f)}</li>`).join("")}</ul></div>` : ""}
      ${r.nextStep ? `<p><strong>Next:</strong> ${md(r.nextStep)}</p>` : ""}
      <p class="muted small">Claude's grade is a guide. Compare with the model answer too.</p>
    </div>`;
    if (task.onScore) task.onScore(score, outOf);
  } catch (e) {
    const msg = aiErrorText(e);
    box.innerHTML = msg ? `<div class="note bad">${esc(msg)}</div>` : "";
  } finally {
    if (btn) btn.disabled = false;
  }
}

function askBox(context) {
  return `<details class="ask ai-only" data-context="${esc(context)}">
    <summary>Ask Claude about this</summary>
    <textarea placeholder="e.g. Why is B wrong? Explain it like I'm new to MapReduce." aria-label="Your question"></textarea>
    <div class="row" style="margin-top:8px"><button class="btn small accent" data-act="ask">Ask</button><button class="btn small ghost" data-act="ask-stop" hidden>Stop</button></div>
    <div class="answer-text"></div>
  </details>`;
}
let askCtl = null;
async function runAsk(details) {
  if (!AI) return;
  const ta = $("textarea", details), out = $(".answer-text", details);
  const stop = $('[data-act="ask-stop"]', details);
  const qtext = ta.value.trim() || "Explain this so I understand it for the exam.";
  const prompt = `You are a patient tutor helping a student cram for the CP422 "Programming for Big Data" midterm. Answer in under 180 words, plain text, using the course's own terms and the way the lecture slides teach it (the professor only accepts his way). If code helps, keep it short and in the slide style.

Slide patterns:
${SLIDE_RULES}

CONTEXT (what the student is looking at):
${details.dataset.context}

STUDENT'S QUESTION:
${qtext}`;
  askCtl = new AbortController();
  out.textContent = "Thinking…";
  stop.hidden = false;
  try {
    await AI(prompt, { cache: false, signal: askCtl.signal, onText: ({ text }) => { out.textContent = text; } });
  } catch (e) {
    const msg = aiErrorText(e);
    out.textContent = (e && e.text ? e.text + "\n\n" : "") + msg;
  } finally {
    stop.hidden = true;
  }
}

/* ---------------- routing ---------------- */
const view = $("#view");
const state = {
  view: "plan",
  notesTopic: "mapreduce", notesQuery: "",
  quiz: null, quizSetup: { topics: CP.topics.map(t => t.id), kind: "all", count: 20, mode: "all" },
  track: null, ex: null,
  sa: null,
  mock: null
};
function go(name, opts = {}) {
  if (!VIEWS.includes(name)) name = "plan";
  state.view = name;
  if (name === "code") {
    if (opts.id === "slide-style") { state.ex = null; state.showStyle = true; }
    else if (opts.id && CODE[opts.id]) { state.ex = opts.id; state.track = CODE[opts.id].track; }
    else if (opts.track) { state.track = opts.track; state.ex = null; }
  }
  if (name === "short" && opts.id) state.sa = opts.id;
  if (name === "quiz" && opts.topic) { state.quiz = null; state.quizSetup.topics = [opts.topic]; }
  try { if (location.hash !== "#" + name) history.replaceState(null, "", "#" + name); } catch (e) { /* ignore */ }
  render();
  window.scrollTo({ top: 0 });
}
function clearHandlers() { view.onclick = view.onchange = view.oninput = view.onkeydown = null; }
function render() {
  clearHandlers();
  $$(".tab").forEach(t => t.setAttribute("aria-current", t.dataset.view === state.view ? "page" : "false"));
  const m = mistakes().length;
  const qt = $('.tab[data-view="quiz"]');
  qt.innerHTML = "Quiz" + (m ? `<span class="n">${m} to redo</span>` : "");
  ({ plan: renderPlan, notes: renderNotes, quiz: renderQuiz, code: renderCode, short: renderShort, mock: renderMock })[state.view]();
}
$("#tabs").addEventListener("click", e => { const b = e.target.closest(".tab"); if (b) go(b.dataset.view); });

/* ---------------- countdown ---------------- */
const examStart = new Date(CP.exam.start).getTime();
const examEnd = new Date(CP.exam.end).getTime();
function countdownParts() {
  const ms = examStart - Date.now();
  if (Date.now() > examEnd) return { done: true, text: "Midterm done" };
  if (ms <= 0) return { live: true, text: "Midterm in progress" };
  const d = Math.floor(ms / 86400000), h = Math.floor(ms % 86400000 / 3600000), m = Math.floor(ms % 3600000 / 60000);
  return { d, h, m, text: d ? `${d}d ${h}h to midterm` : `${h}h ${m}m to midterm` };
}
function tickCountdown() {
  const c = countdownParts();
  $("#countdown-pill").textContent = c.text;
  const big = $("#countdown-big");
  if (big) big.innerHTML = c.done || c.live ? esc(c.text) : (c.d ? `${c.d}<small>d</small> ${c.h}<small>h</small> ${c.m}<small>m</small>` : `${c.h}<small>h</small> ${c.m}<small>m</small>`);
}
setInterval(tickCountdown, 30000);
tickCountdown();

/* ================= PLAN ================= */
function topicStats(topicId) {
  const qs = QUESTIONS.filter(q => q.topic === topicId);
  let seen = 0, n = 0, c = 0;
  qs.forEach(q => { const r = S.q[q.id]; if (r) { seen++; n += r.n; c += r.c; } });
  return { total: qs.length, seen, acc: pct(c, n), n };
}
function trackStats(trackId) {
  const ex = CP.code.filter(c => c.track === trackId);
  return { total: ex.length, done: ex.filter(c => S.code[c.id] && S.code[c.id].done).length };
}
function renderPlan() {
  const e = CP.exam;
  const m = mistakes().length;
  view.innerHTML = `
  <section class="hero fade-in">
    <div>
      <p class="eyebrow">${esc(e.where)} · Fri Oct 23 · 10:30-11:20</p>
      <h1 style="margin-top:6px">CP422 Midterm</h1>
      <p class="when">${esc(e.minutes)} minutes. Covers everything up to the start of Spark Streaming.</p>
    </div>
    <div class="stack">
      <div class="countdown" id="countdown-big"></div>
      <div class="blueprint" role="img" aria-label="Marks: multiple choice 20%, true/false 20%, code 40%, short answer 20%">
        <div class="mcq"><b>20%</b><span>10 MCQ</span></div>
        <div class="tf"><b>20%</b><span>10 T/F</span></div>
        <div class="code"><b>40%</b><span>2 coding questions</span></div>
        <div class="sa"><b>20%</b><span>Short answer</span></div>
      </div>
    </div>
  </section>

  <div class="grid-2">
    <section class="card">
      <h2>What the prof said in class</h2>
      <p class="muted small">From your notes. Each one links to practice.</p>
      <ol class="clean flags">${CP.flags.map((f, i) => `<li><span class="when">${esc(f.when)}</span><span>${md(f.text)}</span><button class="go" data-flag="${i}">Practise this →</button></li>`).join("")}</ol>
    </section>
    <div class="grid">
      <section class="card">
        <h2>Two-day plan</h2>
        <ul class="clean checklist">${CP.plan.map(p => `<li><input type="checkbox" id="plan-${p.id}" data-plan="${p.id}" ${S.plan[p.id] ? "checked" : ""}><div><label for="plan-${p.id}"><span class="day">${esc(p.day)}</span>${md(p.text)}</label></div></li>`).join("")}</ul>
      </section>
      <section class="card">
        <div class="row spread"><h2>Where you stand</h2>${m ? `<button class="btn small accent" data-act="redo">Redo ${m} missed</button>` : ""}</div>
        <div class="mastery">${CP.topics.map(t => { const s = topicStats(t.id); return `<div class="mastery-row"><span>${esc(t.name)}</span><div class="meter" title="${s.seen} of ${s.total} questions tried"><i style="width:${pct(s.seen, s.total)}%"></i></div><span class="num">${s.n ? s.acc + "% right" : "not started"}</span></div>`; }).join("")}</div>
        <div class="mastery" style="margin-top:14px">${CP.tracks.map(t => { const s = trackStats(t.id); return `<div class="mastery-row"><span>Code: ${esc(t.name)}</span><div class="meter"><i style="width:${pct(s.done, s.total)}%;background:var(--reduce)"></i></div><span class="num">${s.done}/${s.total} done</span></div>`; }).join("")}</div>
        ${S.mocks.length ? `<p class="small muted">Mock exams: ${S.mocks.slice(-5).map(x => `<strong>${esc(x.total)}%</strong> (${esc(x.date)})`).join(" · ")}</p>` : `<p class="small muted">No mock exam yet. Do one Thursday evening.</p>`}
      </section>
    </div>
  </div>

  <section class="card">
    <h2>How to use this</h2>
    <div class="prose"><ul>
      <li><strong>Quiz</strong> drills the MCQ and true/false parts with explanations. Anything you miss goes into a redo pile.</li>
      <li><strong>Code Lab</strong> builds coding up in five levels: read code → fill in blanks → put lines in order → write with hints → full exam-style questions with a data-flow trace and rubric.</li>
      <li><strong>Short answer</strong> practises the compare / explain-why question. Check yourself against key points and a model answer.</li>
      <li><strong>Mock exam</strong> is the real format: 10 MCQ, 10 T/F, 2 coding, 1 short answer in 50 minutes.</li>
      <li class="ai-only">Claude can grade your written code and short answers and answer follow-up questions. It uses your own Claude usage.</li>
    </ul></div>
    <p class="note warn small">Answer every coding question the way the slides do it. The prof said he only marks his way as correct, so every model answer here follows the slide patterns (see Code Lab → "Answer his way").</p>
  </section>`;
  tickCountdown();
  view.onclick = ev => {
    const f = ev.target.closest("[data-flag]");
    if (f) { const g = CP.flags[+f.dataset.flag].go; go(g.view, g); return; }
    if (ev.target.closest('[data-act="redo"]')) { startQuiz({ mode: "mistakes" }); }
  };
  view.onchange = ev => {
    const c = ev.target.closest("[data-plan]");
    if (c) { S.plan[c.dataset.plan] = c.checked; save(); }
  };
}

/* ================= NOTES ================= */
function renderNotes() {
  clearHandlers();
  const tabs = [...CP.topics.map(t => ({ id: t.id, name: t.name })), { id: "compare", name: "Compare tables" }, { id: "style", name: "Slide patterns" }];
  const q = state.notesQuery.trim().toLowerCase();
  const highlight = html => {
    if (!q) return html;
    const re = new RegExp("(" + q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + ")", "gi");
    return html.replace(/(<[^>]+>)|([^<]+)/g, (m, tag, text) => tag || text.replace(re, "<mark>$1</mark>"));
  };
  let body = "";
  if (q) {
    const hits = [];
    CP.topics.forEach(t => CP.notes[t.id].forEach(sec => sec.items.forEach(it => { if ((sec.h + " " + it).toLowerCase().includes(q)) hits.push({ t, sec, it }); })));
    body = hits.length ? `<ul class="clean stack">${hits.map(h => `<li class="card" style="box-shadow:none"><p class="eyebrow">${esc(h.t.name)} · ${esc(h.sec.h)}</p><p>${highlight(md(h.it))}</p></li>`).join("")}</ul>` : `<p class="empty">Nothing matches "${esc(state.notesQuery)}".</p>`;
  } else if (state.notesTopic === "compare") {
    body = CP.compare.map(t => `<section class="card"><h3>${esc(t.title)}</h3><div class="table-wrap"><table><thead><tr>${t.cols.map(c => `<th>${esc(c)}</th>`).join("")}</tr></thead><tbody>${t.rows.map(r => `<tr>${r.map((c, i) => i === 0 ? `<th scope="row">${md(c)}</th>` : `<td>${md(c)}</td>`).join("")}</tr>`).join("")}</tbody></table></div></section>`).join("");
  } else if (state.notesTopic === "style") {
    body = styleCard();
  } else {
    const t = TOPIC[state.notesTopic];
    body = `<section class="card"><p class="eyebrow">${esc(t.deck)}</p>${CP.notes[t.id].map(sec => `<div class="note-section prose"><h3>${esc(sec.h)}</h3><ul>${sec.items.map(i => `<li>${md(i)}</li>`).join("")}</ul></div>`).join("")}</section>
      <div class="row"><button class="btn" data-quiz-topic="${t.id}">Quiz me on ${esc(t.name)}</button></div>`;
  }
  view.innerHTML = `
    <div class="row spread"><h1>Notes</h1><input class="search" id="notes-search" type="search" placeholder="Search notes (e.g. combiner, NodeManager)" value="${esc(state.notesQuery)}" aria-label="Search notes"></div>
    <div class="row" role="group" aria-label="Topic">${tabs.map(t => `<button class="chip" data-topic="${t.id}" aria-pressed="${!q && state.notesTopic === t.id}">${esc(t.name)}</button>`).join("")}</div>
    <div class="grid fade-in" id="notes-body">${body}</div>`;
  const input = $("#notes-search");
  input.oninput = () => { state.notesQuery = input.value; const pos = input.selectionStart; renderNotes(); const i2 = $("#notes-search"); i2.focus(); i2.setSelectionRange(pos, pos); };
  view.onclick = ev => {
    const c = ev.target.closest("[data-topic]");
    if (c) { state.notesTopic = c.dataset.topic; state.notesQuery = ""; renderNotes(); return; }
    const qz = ev.target.closest("[data-quiz-topic]");
    if (qz) { state.quizSetup.topics = [qz.dataset.quizTopic]; state.quiz = null; go("quiz"); }
  };
  view.onchange = null;
}
function styleCard() {
  return `<section class="card" id="slide-style">
    <h2>Answer his way</h2>
    <p class="muted">The prof only marks the approach from his slides and labs as correct, and he grades logic, not syntax. Reuse these patterns in every coding answer.</p>
    <div class="grid-2">${CP.slideStyle.map(t => `<div class="prose"><h3>${esc(t.name)}</h3><ul>${t.items.map(i => `<li>${md(i)}</li>`).join("")}</ul></div>`).join("")}</div>
  </section>`;
}

/* ================= QUIZ ================= */
function startQuiz(opts = {}) {
  const setup = { ...state.quizSetup, ...opts };
  let pool;
  if (setup.mode === "mistakes") pool = mistakes();
  else if (setup.mode === "review") pool = QUESTIONS.filter(q => q.review);
  else {
    pool = QUESTIONS.filter(q => setup.topics.includes(q.topic) && (setup.kind === "all" || q.kind === setup.kind));
    if (setup.mode === "new") pool = pool.filter(q => !S.q[q.id]);
  }
  if (!pool.length) { state.quiz = { empty: true }; state.view = "quiz"; render(); return; }
  // Unseen and previously-missed questions first, then the rest.
  const weight = q => { const r = S.q[q.id]; return !r ? 0 : r.last === 0 ? 1 : 2; };
  pool = shuffle(pool).sort((a, b) => weight(a) - weight(b));
  const count = setup.mode === "mistakes" || setup.mode === "review" ? pool.length : Math.min(setup.count === "all" ? pool.length : +setup.count, pool.length);
  const items = pool.slice(0, count).map(q => q.kind === "mcq" ? { ...q, order: shuffle(q.options.map((_, i) => i)) } : q);
  state.quiz = { items, i: 0, answers: [], done: false, label: setup.mode === "mistakes" ? "Redo missed questions" : setup.mode === "review" ? "Prof's review MCQs" : "Practice quiz" };
  state.view = "quiz";
  try { history.replaceState(null, "", "#quiz"); } catch (e) { /* ignore */ }
  render();
  window.scrollTo({ top: 0 });
}
function renderQuiz() {
  clearHandlers();
  const z = state.quiz;
  if (!z || z.empty) return renderQuizSetup(z && z.empty);
  if (z.done) return renderQuizResults();
  const q = z.items[z.i];
  const answered = z.answers[z.i];
  const correctSoFar = z.answers.filter(a => a && a.ok).length;
  let opts;
  if (q.kind === "mcq") {
    opts = `<div class="options">${q.order.map((oi, pos) => {
      let cls = "";
      if (answered) { if (oi === q.answer) cls = "right"; else if (oi === answered.pick) cls = "wrong"; }
      return `<button class="opt ${cls}" data-pick="${oi}" ${answered ? "disabled" : ""}><span class="key">${LETTERS[pos]}</span><span>${md(q.options[oi])}</span></button>`;
    }).join("")}</div>`;
  } else {
    opts = `<div class="tf-row">${[true, false].map((v, pos) => {
      let cls = "";
      if (answered) { if (v === q.answer) cls = "right"; else if (v === answered.pick) cls = "wrong"; }
      return `<button class="opt ${cls}" data-pick="${v}" ${answered ? "disabled" : ""}><span class="key">${v ? "T" : "F"}</span><span>${v ? "True" : "False"}</span></button>`;
    }).join("")}</div>`;
  }
  const fb = answered ? `<div class="note ${answered.ok ? "good" : "bad"} fade-in"><strong>${answered.ok ? "Correct." : "Not quite."}</strong> ${q.kind === "tf" ? `The statement is <strong>${q.answer ? "true" : "false"}</strong>. ` : ""}${md(q.explain)}<div class="src" style="margin-top:6px">Source: ${esc(q.src)}</div></div>
    ${askBox(`${q.kind === "mcq" ? "Multiple choice" : "True/false"} question: ${q.q}\n${q.kind === "mcq" ? q.options.map((o, i) => `${LETTERS[i]}. ${o}`).join("\n") + `\nCorrect answer: ${q.options[q.answer]}` : `Correct answer: ${q.answer}`}\nStudent picked: ${q.kind === "mcq" ? q.options[answered.pick] : answered.pick}\nExplanation shown: ${q.explain}`)}
    <div class="row"><button class="btn primary" data-act="next">${z.i + 1 < z.items.length ? "Next question" : "See results"} <kbd class="muted small">Enter</kbd></button></div>` : "";
  view.innerHTML = `
    <div class="progress-top"><span class="small muted">${esc(z.label)} · ${z.i + 1} / ${z.items.length}</span><div class="meter"><i style="width:${pct(z.i + (answered ? 1 : 0), z.items.length)}%"></i></div><span class="small muted">${correctSoFar} right</span></div>
    <section class="card fade-in">
      <div class="qmeta"><span class="tag">${esc(TOPIC[q.topic].name)}</span><span class="tag ${q.kind === "mcq" ? "map" : "shuffle"}">${q.kind === "mcq" ? "Multiple choice" : "True / False"}</span>${q.review ? '<span class="tag reduce">From the prof\'s review sheet</span>' : ""}</div>
      <p class="qtext">${md(q.q)}</p>
      ${q.kind === "mcq" ? '<p class="small muted">Several options may be partly true. Pick the best one.</p>' : ""}
      ${opts}
      ${fb}
    </section>
    <div class="row"><button class="btn ghost small" data-act="quit">End quiz</button></div>`;
  view.onclick = ev => {
    const p = ev.target.closest("[data-pick]");
    if (p && !answered) {
      const pick = q.kind === "mcq" ? +p.dataset.pick : p.dataset.pick === "true";
      const ok = pick === q.answer;
      z.answers[z.i] = { pick, ok };
      record(q.id, ok);
      renderQuiz();
      return;
    }
    if (ev.target.closest('[data-act="next"]')) { nextQ(); return; }
    if (ev.target.closest('[data-act="quit"]')) { z.done = true; z.items = z.items.slice(0, z.answers.filter(Boolean).length); renderQuiz(); return; }
    if (ev.target.closest('[data-act="ask"]')) { runAsk(ev.target.closest("details")); return; }
    if (ev.target.closest('[data-act="ask-stop"]')) { askCtl && askCtl.abort(); }
  };
  view.onchange = null;
}
function nextQ() {
  const z = state.quiz;
  if (z.i + 1 < z.items.length) z.i++; else z.done = true;
  render();
  window.scrollTo({ top: 0 });
}
document.addEventListener("keydown", ev => {
  if (state.view !== "quiz" || !state.quiz || state.quiz.done || state.quiz.empty) return;
  if (ev.target.matches("input, textarea")) return;
  const z = state.quiz, q = z.items[z.i];
  if (z.answers[z.i]) { if (ev.key === "Enter") { ev.preventDefault(); nextQ(); } return; }
  const k = ev.key.toUpperCase();
  let btn = null;
  if (q.kind === "mcq") { const pos = "ABCD".indexOf(k) >= 0 ? "ABCD".indexOf(k) : "1234".indexOf(ev.key); if (pos >= 0) btn = $$(".opt", view)[pos]; }
  else if (k === "T" || k === "F") btn = $(`.opt[data-pick="${k === "T"}"]`, view);
  if (btn) { ev.preventDefault(); btn.click(); }
});
function renderQuizSetup(empty) {
  clearHandlers();
  const su = state.quizSetup;
  const m = mistakes().length;
  const poolSize = QUESTIONS.filter(q => su.topics.includes(q.topic) && (su.kind === "all" || q.kind === su.kind)).length;
  view.innerHTML = `
    <h1>Quiz</h1>
    ${empty ? '<p class="note warn">No questions match that choice yet. Pick different topics or a different mode.</p>' : ""}
    <div class="grid-2">
      <section class="card stack">
        <h2>Build a quiz</h2>
        <div><p class="eyebrow">Topics</p><div class="row" style="margin-top:6px">${CP.topics.map(t => `<button class="chip" data-qtopic="${t.id}" aria-pressed="${su.topics.includes(t.id)}">${esc(t.name)}</button>`).join("")}</div>
          <div class="row" style="margin-top:6px"><button class="btn small ghost" data-act="all-topics">All topics</button></div></div>
        <div><p class="eyebrow">Question type</p><div class="row" style="margin-top:6px">${[["all", "Both"], ["mcq", "Multiple choice"], ["tf", "True / False"]].map(([v, l]) => `<button class="chip" data-kind="${v}" aria-pressed="${su.kind === v}">${l}</button>`).join("")}</div></div>
        <div><p class="eyebrow">How many</p><div class="row" style="margin-top:6px">${["10", "20", "40", "all"].map(v => `<button class="chip" data-count="${v}" aria-pressed="${String(su.count) === v}">${v === "all" ? "All " + poolSize : v}</button>`).join("")}</div></div>
        <div class="row"><button class="btn primary" data-act="start">Start quiz</button><span class="small muted">${poolSize} questions in this pool. Unseen and missed ones come first.</span></div>
      </section>
      <section class="card stack">
        <h2>Quick drills</h2>
        <button class="btn" data-act="review" style="justify-content:space-between"><span>The prof's 10 review MCQs</span><span class="tag reduce">answers posted</span></button>
        <button class="btn" data-act="mistakes" ${m ? "" : "disabled"} style="justify-content:space-between"><span>Redo what I got wrong</span><span class="tag ${m ? "bad" : ""}">${m}</span></button>
        <button class="btn" data-act="new" style="justify-content:space-between"><span>Only questions I haven't seen</span><span class="tag">${QUESTIONS.filter(q => !S.q[q.id]).length}</span></button>
        <p class="small muted">Keyboard: A-D or 1-4 to answer, T / F for true/false, Enter for next.</p>
      </section>
    </div>`;
  view.onclick = ev => {
    const t = ev.target.closest("[data-qtopic]");
    if (t) { const id = t.dataset.qtopic; su.topics = su.topics.includes(id) ? su.topics.filter(x => x !== id) : [...su.topics, id]; if (!su.topics.length) su.topics = [id]; renderQuizSetup(); return; }
    const k = ev.target.closest("[data-kind]"); if (k) { su.kind = k.dataset.kind; renderQuizSetup(); return; }
    const c = ev.target.closest("[data-count]"); if (c) { su.count = c.dataset.count; renderQuizSetup(); return; }
    const a = ev.target.closest("[data-act]"); if (!a) return;
    if (a.dataset.act === "all-topics") { su.topics = CP.topics.map(t => t.id); renderQuizSetup(); }
    if (a.dataset.act === "start") startQuiz({ mode: "all" });
    if (a.dataset.act === "review") startQuiz({ mode: "review" });
    if (a.dataset.act === "mistakes") startQuiz({ mode: "mistakes" });
    if (a.dataset.act === "new") startQuiz({ mode: "new", topics: CP.topics.map(t => t.id), kind: "all" });
  };
  view.onchange = null;
}
function renderQuizResults() {
  clearHandlers();
  const z = state.quiz;
  const n = z.items.length, right = z.answers.filter(a => a && a.ok).length;
  const byTopic = {};
  z.items.forEach((q, i) => { const b = byTopic[q.topic] || (byTopic[q.topic] = { n: 0, c: 0 }); b.n++; if (z.answers[i] && z.answers[i].ok) b.c++; });
  const missed = z.items.filter((q, i) => z.answers[i] && !z.answers[i].ok);
  if (!n) { state.quiz = null; renderQuiz(); return; }
  view.innerHTML = `
    <h1>Quiz results</h1>
    <section class="card">
      <div class="row"><div class="result-big">${pct(right, n)}%</div><p>${right} of ${n} right. ${pct(right, n) >= 80 ? "Strong. Move to the Code Lab." : pct(right, n) >= 60 ? "Close. Redo the misses below, then try again." : "Read the Notes for the weak topics, then redo the misses."}</p></div>
      <div class="mastery">${Object.entries(byTopic).map(([t, b]) => `<div class="mastery-row"><span>${esc(TOPIC[t].name)}</span><div class="meter"><i style="width:${pct(b.c, b.n)}%;background:${pct(b.c, b.n) >= 70 ? "var(--good)" : "var(--shuffle)"}"></i></div><span class="num">${b.c}/${b.n}</span></div>`).join("")}</div>
      <div class="row"><button class="btn primary" data-act="again">New quiz</button>${missed.length ? `<button class="btn accent" data-act="retry">Retry the ${missed.length} I missed</button>` : ""}</div>
    </section>
    ${missed.length ? `<section class="card"><h2>Review your misses</h2><ol class="clean stack">${missed.map(q => `<li class="note"><p><strong>${md(q.q)}</strong></p><p style="margin-top:4px">Answer: <strong>${q.kind === "mcq" ? md(q.options[q.answer]) : q.answer ? "True" : "False"}</strong>. ${md(q.explain)}</p><p class="src">${esc(q.src)}</p></li>`).join("")}</ol></section>` : ""}`;
  view.onclick = ev => {
    const a = ev.target.closest("[data-act]"); if (!a) return;
    if (a.dataset.act === "again") { state.quiz = null; renderQuiz(); }
    if (a.dataset.act === "retry") {
      state.quiz = { items: missed.map(q => q.kind === "mcq" ? { ...q, order: shuffle(q.options.map((_, i) => i)) } : q), i: 0, answers: [], done: false, label: "Retry missed" };
      renderQuiz(); window.scrollTo({ top: 0 });
    }
  };
}

/* ================= CODE LAB ================= */
function exStatus(ex) {
  const st = S.code[ex.id];
  if (!st) return "";
  if (st.done) return `<span class="status done">✓ ${st.score != null ? esc(st.score) + "/" + esc(st.outOf) : "done"}</span>`;
  if (st.draft) return '<span class="status muted">draft</span>';
  return "";
}
function renderCode() {
  clearHandlers();
  if (state.ex && CODE[state.ex]) return renderExercise(CODE[state.ex]);
  const tr = state.track;
  const tracksHtml = `<div class="grid-4 tracks">${CP.tracks.map(t => {
    const ex = CP.code.filter(c => c.track === t.id);
    const ladder = CP.levels.map(l => { const items = ex.filter(c => c.level === l.n); const d = items.filter(c => S.code[c.id] && S.code[c.id].done).length; return `<i class="${d === items.length && items.length ? "full" : d ? "part" : ""}" title="Level ${l.n}: ${d}/${items.length}"></i>`; }).join("");
    const s = trackStats(t.id);
    return `<button class="card track" data-track="${t.id}" aria-pressed="${tr === t.id}"><h3>${esc(t.name)}</h3><p class="small muted">${esc(t.blurb)}</p><div class="ladder">${ladder}</div><span class="small muted">${s.done}/${s.total} done</span></button>`;
  }).join("")}</div>`;
  let list = "";
  if (tr) {
    const ex = CP.code.filter(c => c.track === tr);
    list = `<section class="card fade-in"><div class="row spread"><h2>${esc(TRACK[tr].name)}</h2><span class="small muted">Work top to bottom: each level builds on the last.</span></div>
      ${CP.levels.map(l => { const items = ex.filter(c => c.level === l.n); if (!items.length) return ""; return `<div class="level-head"><span class="lvl l${l.n}">L${l.n}</span><h3>${esc(l.name)}</h3><span class="small muted">${esc(l.what)}</span></div><ul class="clean exlist">${items.map(c => `<li><button data-ex="${c.id}"><span class="lvl l${c.level}">${c.level}</span><span>${esc(c.title)}${c.exam ? ' <span class="tag reduce">mock exam pool</span>' : ""}</span>${exStatus(c) || '<span class="status muted">→</span>'}</button></li>`).join("")}</ul>`; }).join("")}
    </section>`;
  }
  view.innerHTML = `
    <div class="row spread"><h1>Code Lab</h1><span class="small muted">2 coding questions = 40% of the midterm</span></div>
    ${tr ? "" : `<p class="note">New to this? Start with <strong>Hadoop Streaming</strong>: it's plain Python with stdin and stdout. Then do <strong>Java MapReduce</strong> (same logic, more boilerplate), then the two Spark tracks.</p>`}
    ${tracksHtml}
    ${list}
    <details class="card" ${state.showStyle ? "open" : ""} id="style-details"><summary>Answer his way: the slide patterns to reuse</summary>${styleCard()}</details>`;
  if (state.showStyle) { state.showStyle = false; setTimeout(() => $("#style-details") && $("#style-details").scrollIntoView({ block: "start" }), 0); }
  view.onclick = ev => {
    const t = ev.target.closest("[data-track]");
    if (t) { state.track = state.track === t.dataset.track ? null : t.dataset.track; renderCode(); return; }
    const x = ev.target.closest("[data-ex]");
    if (x) { state.ex = x.dataset.ex; renderCode(); window.scrollTo({ top: 0 }); }
  };
  view.onchange = null;
}
function traceHtml(trace) {
  if (!trace) return "";
  return `<div><h3>How the data flows</h3><div class="trace" style="margin-top:8px">${trace.map(p => `<div class="phase ${p.phase}"><h4>${esc(p.label)}</h4>${p.rows.map(r => `<div>${esc(r)}</div>`).join("")}</div>`).join("")}</div></div>`;
}
function renderExercise(ex) {
  clearHandlers();
  const siblings = CP.code.filter(c => c.track === ex.track);
  const idx = siblings.indexOf(ex);
  const prev = siblings[idx - 1], next = siblings[idx + 1];
  const level = CP.levels[ex.level - 1];
  const head = `
    <div class="crumbs"><button data-act="to-lab">Code Lab</button><span>›</span><button data-act="to-track">${esc(TRACK[ex.track].name)}</button><span>›</span><span>Level ${ex.level}: ${esc(level.name)}</span></div>
    <div class="row"><span class="lvl l${ex.level}">L${ex.level}</span><h1 style="font-size:clamp(1.4rem,4vw,2rem)">${esc(ex.title)}</h1></div>`;
  const nav = `<div class="row spread"><button class="btn" data-act="prev" ${prev ? "" : "disabled"}>← ${prev ? esc(prev.title) : "Start of track"}</button><button class="btn primary" data-act="next" ${next ? "" : "disabled"}>${next ? "Next: " + esc(next.title) : "End of track"} →</button></div>`;
  let body = "";
  if (ex.type === "mcq") body = exMcq(ex);
  else if (ex.type === "fill") body = exFill(ex);
  else if (ex.type === "parsons") body = exParsons(ex);
  else body = exWrite(ex);
  view.innerHTML = `${head}<section class="card fade-in" id="ex-card">${body}</section>${nav}`;
  wireExercise(ex);
  view.onclick = ev => {
    const pk = ev.target.closest("[data-pk]");
    if (pk && ex.type === "parsons") {
      const st = codeState(ex.id), k = pk.dataset.pk;
      if (pk.dataset.where === "pool") { st.pool = st.pool.filter(x => x !== k); st.ans.push(k); }
      else { st.ans = st.ans.filter(x => x !== k); st.pool.push(k); }
      save(); drawParsons(ex); $("#p-msg").innerHTML = "";
      return;
    }
    const a = ev.target.closest("[data-act]");
    if (!a) return;
    const act = a.dataset.act;
    if (act === "to-lab") { state.ex = null; state.track = null; renderCode(); }
    else if (act === "to-track") { state.ex = null; renderCode(); }
    else if (act === "prev" && prev) { state.ex = prev.id; renderCode(); window.scrollTo({ top: 0 }); }
    else if (act === "next" && next) { state.ex = next.id; renderCode(); window.scrollTo({ top: 0 }); }
    else if (act === "ask") runAsk(a.closest("details"));
    else if (act === "ask-stop") askCtl && askCtl.abort();
    else exAction(ex, act, a);
  };
}
function markDone(ex, score, outOf) {
  const st = codeState(ex.id);
  st.done = true;
  if (score != null) { st.score = score; st.outOf = outOf; }
  save();
}

/* --- L1 read (mcq) --- */
function exMcq(ex) {
  const st = codeState(ex.id);
  const ans = st.pick;
  return `${ex.prompt ? `<p>${md(ex.prompt)}</p>` : ""}
    ${codeBlock(ex.code, ex.lang)}
    <p class="qtext">${md(ex.q)}</p>
    <div class="options">${ex.options.map((o, i) => { let cls = ""; if (ans != null) { if (i === ex.answer) cls = "right"; else if (i === ans) cls = "wrong"; } return `<button class="opt ${cls}" data-act="pick" data-i="${i}" ${ans != null ? "disabled" : ""}><span class="key">${LETTERS[i]}</span><span>${md(o)}</span></button>`; }).join("")}</div>
    ${ans != null ? `<div class="note ${ans === ex.answer ? "good" : "bad"} fade-in"><strong>${ans === ex.answer ? "Correct." : "Not quite."}</strong> ${md(ex.explain)}<div class="src" style="margin-top:6px">Source: ${esc(ex.src)}</div></div>
      ${traceHtml(ex.trace)}
      <div class="row"><button class="btn small ghost" data-act="reset">Try again</button></div>
      ${askBox(`Code:\n${ex.code}\nQuestion: ${ex.q}\nCorrect: ${ex.options[ex.answer]}\nStudent picked: ${ex.options[ans]}\nExplanation: ${ex.explain}`)}` : ""}`;
}

/* --- L2 fill in --- */
function parseFill(code) {
  const parts = []; const re = /⟦([^⟧]*)⟧/g; let last = 0, m;
  while ((m = re.exec(code))) { parts.push({ text: code.slice(last, m.index) }); parts.push({ blank: m[1].split("|") }); last = re.lastIndex; }
  parts.push({ text: code.slice(last) });
  return parts;
}
function exFill(ex) {
  const parts = parseFill(ex.code);
  const st = codeState(ex.id);
  const vals = st.fill || [];
  let bi = 0;
  const html = parts.map(p => {
    if (p.text != null) return hl(p.text, ex.lang);
    const i = bi++;
    const w = Math.max(...p.blank.map(b => b.length)) + 1;
    return `<input class="blank" data-b="${i}" size="${w}" value="${esc(vals[i] || "")}" aria-label="Blank ${i + 1}" autocomplete="off" autocapitalize="off" spellcheck="false">`;
  }).join("");
  return `<p>${md(ex.prompt)}</p>
    ${ex.data ? `<p class="eyebrow">Sample input</p>${codeBlock(ex.data, "text", "data")}` : ""}
    <pre class="code"><code>${html}</code></pre>
    <div class="row"><button class="btn primary" data-act="check-fill">Check</button><button class="btn" data-act="reveal-fill">Show answers</button><span class="small muted" id="fill-msg"></span></div>
    <div id="fill-after" hidden><div class="note">${md(ex.explain)}<div class="src" style="margin-top:6px">Source: ${esc(ex.src)}</div></div>
    ${askBox(`Fill-in-the-blank exercise (${TRACK[ex.track].name}). Code with blanks marked ⟦answer⟧:\n${ex.code}\nExplanation: ${ex.explain}`)}</div>`;
}
const norm = (s, ci) => { let v = String(s).trim().replace(/\s+/g, " "); if (ci) v = v.toLowerCase(); return v; };

/* --- L3 parsons --- */
function exParsons(ex) {
  const st = codeState(ex.id);
  if (!st.pool) {
    const all = [...ex.lines.map((l, i) => ({ t: l[0], g: l[1], k: "l" + i })), ...(ex.distractors || []).map((t, i) => ({ t, g: -1, k: "d" + i }))];
    st.pool = shuffle(all.map(x => x.k)); st.ans = [];
  }
  return `<p>${md(ex.prompt)}</p>
    <div class="parsons">
      <div><p class="eyebrow">Your program (tap a line to remove it)</p><div class="parsons-col answer" id="p-ans"></div></div>
      <div><p class="eyebrow">Lines (tap to add)</p><div class="parsons-col" id="p-pool"></div></div>
    </div>
    <div class="row"><button class="btn primary" data-act="check-p">Check</button><button class="btn" data-act="reset-p">Start over</button><button class="btn ghost" data-act="show-p">Show solution</button></div>
    <div id="p-msg"></div>`;
}
function parsonsLines(ex) {
  const map = {};
  ex.lines.forEach((l, i) => { map["l" + i] = { t: l[0], g: l[1] }; });
  (ex.distractors || []).forEach((t, i) => { map["d" + i] = { t, g: -1 }; });
  return map;
}
function drawParsons(ex, marks) {
  const st = codeState(ex.id), L = parsonsLines(ex);
  const line = (k, i, inAns) => `<button class="pline ${inAns ? "" : "pool"} ${marks && inAns ? (marks[i] ? "ok" : "no") : ""}" data-pk="${k}" data-where="${inAns ? "ans" : "pool"}">${inAns ? `<span class="ln">${i + 1}</span>` : ""}<span class="pc">${hl(L[k].t, ex.lang)}</span></button>`;
  $("#p-ans").innerHTML = st.ans.length ? st.ans.map((k, i) => line(k, i, true)).join("") : '<p class="empty small">Tap lines on the right to build the program.</p>';
  $("#p-pool").innerHTML = st.pool.length ? st.pool.map((k, i) => line(k, i, false)).join("") : '<p class="empty small">All lines used.</p>';
}

/* --- L4/L5 write --- */
function exWrite(ex) {
  const st = codeState(ex.id);
  const outOf = ex.rubric.reduce((a, r) => a + r.pts, 0);
  return `<p>${md(ex.prompt)}</p>
    ${ex.exam ? `<p class="note warn small">Exam conditions: no imports needed, start from <code>public class</code> / variable definitions, marked on logic. Aim for about 12 minutes.</p>` : ""}
    <div class="grid-2">
      ${ex.data ? `<div><p class="eyebrow">Sample input</p>${codeBlock(ex.data, "text", "data")}</div>` : ""}
      ${ex.expected ? `<div><p class="eyebrow">Expected output</p>${codeBlock(ex.expected, "text", "data")}</div>` : ""}
    </div>
    <div><label class="eyebrow" for="editor">Your answer</label>
    <textarea class="editor" id="editor" spellcheck="false" autocapitalize="off" autocomplete="off">${esc(st.draft != null ? st.draft : ex.starter || "")}</textarea></div>
    <div class="row">
      <button class="btn" data-act="hint">Hint ${ex.hints ? `(${Math.min(st.hints || 0, ex.hints.length)}/${ex.hints.length})` : ""}</button>
      <button class="btn" data-act="quick">Quick check</button>
      <button class="btn accent ai-only" data-act="grade">Grade with Claude</button>
      <button class="btn primary" data-act="solution">Show model answer</button>
      <button class="btn ghost small" data-act="reset-editor">Reset</button>
    </div>
    <div id="hints">${(ex.hints || []).slice(0, st.hints || 0).map((h, i) => `<div class="note fade-in" style="margin-top:8px"><strong>Hint ${i + 1}.</strong> ${md(h)}</div>`).join("")}</div>
    <div id="quick" hidden></div>
    <div id="ai-out" hidden></div>
    <div id="sol" ${st.showSol ? "" : "hidden"}>${solutionHtml(ex, outOf)}</div>`;
}
function solutionHtml(ex, outOf) {
  const st = codeState(ex.id);
  const ticks = st.ticks || [];
  return `<hr style="border:0;border-top:1px solid var(--line);margin:18px 0">
    <div class="stack">
      <h3>Model answer (slide style)</h3>
      ${codeBlock(ex.solution, ex.lang)}
      ${ex.alt ? `<details><summary>${esc(ex.alt.label)}</summary><div style="margin-top:8px">${codeBlock(ex.alt.code, ex.lang)}</div></details>` : ""}
      <div class="note">${md(ex.explain)}<div class="src" style="margin-top:6px">Source: ${esc(ex.src)}</div></div>
      ${traceHtml(ex.trace)}
      <div><h3>Mark yourself</h3><p class="small muted">Tick what your answer gets right. Logic counts, not syntax.</p>
        <ul class="clean rubric">${ex.rubric.map((r, i) => `<li><input type="checkbox" id="rb-${i}" data-rb="${i}" ${ticks[i] ? "checked" : ""}><label for="rb-${i}">${md(r.text)}</label><span class="pts">${r.pts} pt${r.pts > 1 ? "s" : ""}</span></li>`).join("")}</ul>
        <div class="row" style="margin-top:8px"><strong id="self-score">${selfScore(ex)}/${outOf}</strong><button class="btn small primary" data-act="save-self">Save score and mark done</button></div>
      </div>
      ${askBox(`Coding exercise: ${ex.title}\n${ex.prompt}\nModel answer:\n${ex.solution}\nStudent's answer:\n${codeState(ex.id).draft || "(empty)"}`)}
    </div>`;
}
function selfScore(ex) {
  const ticks = codeState(ex.id).ticks || [];
  return ex.rubric.reduce((a, r, i) => a + (ticks[i] ? r.pts : 0), 0);
}
function runChecks(checks, text) {
  return checks.map(c => { let ok = false; try { ok = new RegExp(c.re, c.flags || "").test(text); } catch (e) { ok = false; } return { ok, label: c.label }; });
}
function checksHtml(results) {
  const n = results.filter(r => r.ok).length;
  return `<div class="note fade-in" style="margin-top:8px"><p><strong>Quick check: ${n}/${results.length}</strong> <span class="small muted">Pattern-based, so it can miss a correct answer written differently. The rubric is the real test.</span></p>
    <ul class="clean checks" style="margin-top:6px">${results.map(r => `<li><span class="mark ${r.ok ? "y" : "n"}">${r.ok ? "✓" : "✗"}</span><span>${md(r.label)}</span></li>`).join("")}</ul></div>`;
}
function langName(ex) { return ex.track === "java" ? "Java MapReduce" : ex.track === "streaming" ? "Python, Hadoop Streaming" : ex.track === "pyspark" ? "PySpark" : "Spark SQL / DataFrames"; }

function wireExercise(ex) {
  if (ex.type === "parsons") drawParsons(ex);
  if (ex.type === "fill") {
    view.oninput = ev => {
      const b = ev.target.closest("input.blank");
      if (b) { const st = codeState(ex.id); st.fill = st.fill || []; st.fill[+b.dataset.b] = b.value; b.classList.remove("ok", "no"); save(); }
    };
    view.onkeydown = ev => { if (ev.key === "Enter" && ev.target.matches("input.blank")) { ev.preventDefault(); exAction(ex, "check-fill"); } };
  } else if (ex.type === "write") {
    const ed = $("#editor");
    ed.addEventListener("input", () => { codeState(ex.id).draft = ed.value; save(); });
    ed.addEventListener("keydown", editorKeys);
    view.oninput = null;
    view.onkeydown = null;
  } else { view.oninput = null; view.onkeydown = null; }
  view.onchange = ev => {
    const rb = ev.target.closest("[data-rb]");
    if (rb) { const st = codeState(ex.id); st.ticks = st.ticks || []; st.ticks[+rb.dataset.rb] = rb.checked; save(); $("#self-score").textContent = selfScore(ex) + "/" + ex.rubric.reduce((a, r) => a + r.pts, 0); }
  };
}
function editorKeys(ev) {
  const ta = ev.target;
  if (ev.key === "Tab" && !ev.shiftKey) {
    ev.preventDefault();
    const s = ta.selectionStart, e = ta.selectionEnd;
    ta.setRangeText("    ", s, e, "end");
    ta.dispatchEvent(new Event("input"));
  } else if (ev.key === "Enter" && !ev.shiftKey && !ev.ctrlKey && !ev.metaKey) {
    const s = ta.selectionStart;
    const lineStart = ta.value.lastIndexOf("\n", s - 1) + 1;
    const line = ta.value.slice(lineStart, s);
    let indent = (line.match(/^[ \t]*/) || [""])[0];
    if (/[:{]\s*$/.test(line)) indent += "    ";
    ev.preventDefault();
    ta.setRangeText("\n" + indent, s, ta.selectionEnd, "end");
    ta.dispatchEvent(new Event("input"));
  }
}

function exAction(ex, act, el) {
  const st = codeState(ex.id);
  if (act === "pick") { st.pick = +el.dataset.i; if (st.pick === ex.answer) st.done = true; save(); renderCode(); return; }
  if (act === "reset") { delete st.pick; save(); renderCode(); return; }
  if (act === "check-fill" || act === "reveal-fill") {
    const parts = parseFill(ex.code).filter(p => p.blank);
    const inputs = $$("input.blank", view);
    let right = 0;
    inputs.forEach((inp, i) => {
      if (act === "reveal-fill") { inp.value = parts[i].blank[0]; st.fill = st.fill || []; st.fill[i] = inp.value; }
      const ci = ex.ci || ex.lang === "sql";
      const ok = parts[i].blank.some(a => norm(a, ci) === norm(inp.value, ci));
      inp.classList.toggle("ok", ok); inp.classList.toggle("no", !ok);
      if (ok) right++;
    });
    const all = right === inputs.length;
    $("#fill-msg").textContent = act === "reveal-fill" ? "Answers filled in. Read the explanation, then clear one and retype it from memory." : all ? `All ${right} correct.` : `${right} of ${inputs.length} correct. Red ones need another look.`;
    if (all || act === "reveal-fill") $("#fill-after").hidden = false;
    if (all && act === "check-fill") markDone(ex);
    save();
    return;
  }
  if (act === "check-p" || act === "reset-p" || act === "show-p") {
    const L = parsonsLines(ex);
    if (act === "reset-p") { delete st.pool; delete st.ans; save(); renderCode(); return; }
    if (act === "show-p") {
      st.ans = ex.lines.map((_, i) => "l" + i);
      st.pool = (ex.distractors || []).map((_, i) => "d" + i);
      save(); drawParsons(ex, st.ans.map(() => true));
      $("#p-msg").innerHTML = `<div class="note fade-in">${md(ex.explain)}<div class="src" style="margin-top:6px">Source: ${esc(ex.src)}</div></div>`;
      return;
    }
    const want = ex.lines.map(l => l[0]);
    const wantG = ex.lines.map(l => l[1]);
    const got = st.ans.map(k => L[k]);
    // A position is right if it holds the expected line, an identical copy of it, or a line from the same order-free group.
    const marks = got.map((x, i) => i < want.length && (x.t === want[i] || x.g === wantG[i]));
    const ok = got.length === want.length && marks.every(Boolean);
    drawParsons(ex, marks);
    const hasDistractor = got.some(x => x.g === -1);
    const firstBad = marks.indexOf(false);
    let msg;
    if (ok) { msg = `<div class="note good fade-in"><strong>Correct!</strong> ${md(ex.explain)}</div>`; markDone(ex); }
    else if (got.length < want.length && firstBad === -1) msg = `<div class="note warn fade-in">So far so good: the first ${got.length} lines are right. The program needs ${want.length} lines.</div>`;
    else msg = `<div class="note bad fade-in">Line ${firstBad + 1} isn't right yet.${hasDistractor ? " Your program also uses a distractor line that doesn't belong." : ""} Tap it to send it back and try again.</div>`;
    $("#p-msg").innerHTML = msg;
    return;
  }
  if (act === "hint") {
    if (!ex.hints) return;
    st.hints = Math.min((st.hints || 0) + 1, ex.hints.length); save();
    $("#hints").innerHTML = ex.hints.slice(0, st.hints).map((h, i) => `<div class="note fade-in" style="margin-top:8px"><strong>Hint ${i + 1}.</strong> ${md(h)}</div>`).join("");
    el.textContent = `Hint (${st.hints}/${ex.hints.length})`;
    if (st.hints >= ex.hints.length) el.disabled = true;
    return;
  }
  if (act === "quick") {
    const q = $("#quick"); q.hidden = false;
    q.innerHTML = checksHtml(runChecks(ex.checks || [], $("#editor").value));
    return;
  }
  if (act === "reset-editor") {
    if (el.dataset.confirm !== "1") { el.dataset.confirm = "1"; el.textContent = "Click again to clear your answer"; return; }
    st.draft = ex.starter || ""; save(); $("#editor").value = st.draft; el.dataset.confirm = ""; el.textContent = "Reset";
    return;
  }
  if (act === "solution") { st.showSol = true; save(); $("#sol").hidden = false; $("#sol").scrollIntoView({ behavior: "smooth", block: "start" }); return; }
  if (act === "save-self") {
    const outOf = ex.rubric.reduce((a, r) => a + r.pts, 0);
    markDone(ex, selfScore(ex), outOf);
    el.textContent = "Saved ✓";
    return;
  }
  if (act === "grade") {
    const outOf = ex.rubric.reduce((a, r) => a + r.pts, 0);
    gradeWithClaude({
      question: ex.prompt + (ex.data ? "\nSample input:\n" + ex.data : "") + (ex.expected ? "\nExpected output:\n" + ex.expected : ""),
      rubric: ex.rubric, reference: ex.solution, langName: langName(ex),
      onScore: score => markDone(ex, score, outOf)
    }, $("#editor").value, $("#ai-out"), el);
  }
}

/* ================= SHORT ANSWER ================= */
function renderShort() {
  clearHandlers();
  const cur = state.sa && SHORT[state.sa];
  const list = `<ul class="clean exlist">${CP.short.map(s => { const st = S.sa[s.id] || {}; return `<li><button data-sa="${s.id}" ${cur && cur.id === s.id ? 'aria-current="true" style="background:var(--sunk)"' : ""}><span class="tag">${esc(TOPIC[s.topic].name)}</span><span>${md(s.q)}</span>${st.score != null ? `<span class="status done">✓ ${esc(st.score)}/${esc(s.marks)}</span>` : st.draft ? '<span class="status muted">draft</span>' : '<span class="status muted">→</span>'}</button></li>`; }).join("")}</ul>`;
  if (!cur) {
    view.innerHTML = `<div class="row spread"><h1>Short answer</h1><span class="small muted">1 question on the midterm, 20%</span></div>
      <p class="note">He said the short answer is about <strong>comparing, describing the difference, and explaining why</strong>. Write 5-8 sentences that hit each key point, using the slide terms. The YARN roles one is flagged for the exam.</p>
      <section class="card">${list}</section>`;
  } else {
    const st = S.sa[cur.id] || (S.sa[cur.id] = {});
    const ticks = st.ticks || [];
    view.innerHTML = `
      <div class="crumbs"><button data-act="sa-list">Short answer</button><span>›</span><span>${esc(TOPIC[cur.topic].name)}</span></div>
      <section class="card fade-in stack">
        <p class="qtext">${md(cur.q)}</p>
        <label class="eyebrow" for="sa-text">Your answer</label>
        <textarea class="sa-area" id="sa-text" placeholder="Answer in full sentences. Name the components, then compare, then explain why.">${esc(st.draft || "")}</textarea>
        <div class="row"><button class="btn accent ai-only" data-act="sa-grade">Grade with Claude</button><button class="btn primary" data-act="sa-model">Show key points and model answer</button><span class="small muted" id="sa-count"></span></div>
        <div id="sa-ai" hidden></div>
        <div id="sa-model" ${st.showModel ? "" : "hidden"} class="stack">
          <h3>Key points (1-2 marks each)</h3>
          <ul class="clean rubric">${cur.points.map((p, i) => `<li><input type="checkbox" id="kp-${i}" data-kp="${i}" ${ticks[i] ? "checked" : ""}><label for="kp-${i}">${md(p)}</label><span class="pts">${(cur.marks / cur.points.length).toFixed(0)} pts</span></li>`).join("")}</ul>
          <div class="row"><strong id="sa-self">${saSelf(cur)}/${cur.marks}</strong><button class="btn small primary" data-act="sa-save">Save my score</button></div>
          <h3>Model answer</h3>
          <div class="note">${md(cur.model)}</div>
          ${askBox(`Short answer question: ${cur.q}\nKey points: ${cur.points.join("; ")}\nModel answer: ${cur.model}\nStudent's answer: ${st.draft || "(empty)"}`)}
        </div>
      </section>
      <details class="card"><summary>All short-answer questions</summary>${list}</details>`;
    const ta = $("#sa-text");
    const count = () => { $("#sa-count").textContent = ta.value.trim() ? ta.value.trim().split(/\s+/).length + " words" : ""; };
    count();
    ta.addEventListener("input", () => { st.draft = ta.value; save(); count(); });
  }
  view.onclick = ev => {
    const s = ev.target.closest("[data-sa]");
    if (s) { state.sa = s.dataset.sa; renderShort(); window.scrollTo({ top: 0 }); return; }
    const a = ev.target.closest("[data-act]"); if (!a) return;
    const act = a.dataset.act;
    if (act === "sa-list") { state.sa = null; renderShort(); }
    if (act === "ask") runAsk(a.closest("details"));
    if (act === "ask-stop") askCtl && askCtl.abort();
    if (!cur) return;
    const st = S.sa[cur.id];
    if (act === "sa-model") { st.showModel = true; save(); $("#sa-model").hidden = false; $("#sa-model").scrollIntoView({ behavior: "smooth", block: "start" }); }
    if (act === "sa-save") { st.score = saSelf(cur); save(); a.textContent = "Saved ✓"; }
    if (act === "sa-grade") {
      gradeWithClaude({
        question: cur.q,
        rubric: cur.points.map(p => ({ pts: +(cur.marks / cur.points.length).toFixed(0), text: p })),
        reference: cur.model, langName: "written short answer",
        onScore: score => { st.score = score; save(); }
      }, $("#sa-text").value, $("#sa-ai"), a);
    }
  };
  view.onchange = ev => {
    const k = ev.target.closest("[data-kp]");
    if (k && cur) { const st = S.sa[cur.id]; st.ticks = st.ticks || []; st.ticks[+k.dataset.kp] = k.checked; save(); $("#sa-self").textContent = saSelf(cur) + "/" + cur.marks; }
  };
}
function saSelf(cur) {
  const ticks = (S.sa[cur.id] || {}).ticks || [];
  const per = cur.marks / cur.points.length;
  return Math.round(cur.points.reduce((a, _, i) => a + (ticks[i] ? per : 0), 0));
}

/* ================= MOCK EXAM ================= */
const MR_FAMILY = CP.code.filter(c => c.exam && (c.track === "java" || c.track === "streaming"));
const SPARK_FAMILY = CP.code.filter(c => c.exam && (c.track === "pyspark" || c.track === "sql"));
function pickSpread(pool, n) {
  // Round-robin across topics so the paper covers everything.
  const byTopic = {};
  shuffle(pool).forEach(q => (byTopic[q.topic] = byTopic[q.topic] || []).push(q));
  const out = []; const keys = shuffle(Object.keys(byTopic));
  while (out.length < n && keys.some(k => byTopic[k].length)) keys.forEach(k => { if (out.length < n && byTopic[k].length) out.push(byTopic[k].pop()); });
  return out;
}
function newMock(timed) {
  const mcq = pickSpread(CP.mcq, 10).map(q => ({ id: q.id, order: shuffle(q.options.map((_, i) => i)) }));
  const tf = pickSpread(CP.tf, 10).map(q => ({ id: q.id }));
  const code = [shuffle(MR_FAMILY)[0].id, shuffle(SPARK_FAMILY)[0].id];
  const sa = shuffle(CP.short)[0].id;
  S.mockCur = { timed, start: Date.now(), mcq, tf, code, sa, ans: { mcq: {}, tf: {}, code: {}, sa: "" }, submitted: false, self: {}, ai: {} };
  save();
}
let mockTimer = null;
function renderMock() {
  clearInterval(mockTimer);
  clearHandlers();
  const M = S.mockCur;
  if (!M) return renderMockStart();
  if (M.submitted) return renderMockResults();
  const left = () => Math.max(0, M.start + CP.exam.minutes * 60000 - Date.now());
  const fmt = ms => { const s = Math.ceil(ms / 1000); return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`; };
  const answeredCount = () => Object.keys(M.ans.mcq).length + Object.keys(M.ans.tf).length + M.code.filter(id => (M.ans.code[id] || "").trim()).length + (M.ans.sa.trim() ? 1 : 0);
  let n = 0;
  const mcqHtml = M.mcq.map(it => { const q = QBYID[it.id]; n++; return `<div class="card stack" id="mq-${n}"><div class="row"><span class="qnum">Q${n}</span><span class="small muted">2 marks</span></div><p class="qtext">${md(q.q)}</p><div class="options">${it.order.map((oi, pos) => `<button class="opt ${M.ans.mcq[it.id] === oi ? "picked" : ""}" data-mcq="${it.id}" data-i="${oi}"><span class="key">${LETTERS[pos]}</span><span>${md(q.options[oi])}</span></button>`).join("")}</div></div>`; }).join("");
  const tfHtml = M.tf.map(it => { const q = QBYID[it.id]; n++; return `<div class="card stack"><div class="row"><span class="qnum">Q${n}</span><span class="small muted">2 marks</span></div><p class="qtext">${md(q.q)}</p><div class="tf-row">${[true, false].map(v => `<button class="opt ${M.ans.tf[it.id] === v ? "picked" : ""}" data-tf="${it.id}" data-v="${v}"><span class="key">${v ? "T" : "F"}</span><span>${v ? "True" : "False"}</span></button>`).join("")}</div></div>`; }).join("");
  const codeHtml = M.code.map(id => { const ex = CODE[id]; n++; return `<div class="card stack"><div class="row"><span class="qnum">Q${n}</span><span class="tag reduce">${esc(langName(ex))}</span><span class="small muted">20 marks</span></div><p>${md(ex.prompt)}</p>${ex.data ? `<p class="eyebrow">Sample input</p>${codeBlock(ex.data, "text", "data")}` : ""}<textarea class="editor mock-code" data-code="${id}" spellcheck="false" aria-label="Answer for question ${n}">${esc(M.ans.code[id] != null ? M.ans.code[id] : ex.starter || "")}</textarea></div>`; }).join("");
  n++;
  const sa = SHORT[M.sa];
  view.innerHTML = `
    ${M.timed ? `<div class="timer" id="timer"><span class="clock" id="clock">${fmt(left())}</span><span class="small">left · <span id="answered">${answeredCount()}</span>/23 answered</span><button class="btn small" data-act="submit">Submit</button></div>` : `<div class="timer"><span class="small">Untimed mock · <span id="answered">${answeredCount()}</span>/23 answered</span><button class="btn small" data-act="submit">Submit</button></div>`}
    <div id="confirm"></div>
    <div class="part-head"><h2>Part A · Multiple choice</h2><span class="muted small">10 × 2 = 20%. Pick the BEST answer.</span></div>${mcqHtml}
    <div class="part-head"><h2>Part B · True / False</h2><span class="muted small">10 × 2 = 20%</span></div>${tfHtml}
    <div class="part-head"><h2>Part C · Write code</h2><span class="muted small">2 × 20 = 40%. No imports; start from the class or variable definitions. Logic is marked, not syntax.</span></div>${codeHtml}
    <div class="part-head"><h2>Part D · Short answer</h2><span class="muted small">20%</span></div>
    <div class="card stack"><div class="row"><span class="qnum">Q${n}</span><span class="small muted">20 marks</span></div><p class="qtext">${md(sa.q)}</p><textarea class="sa-area" id="mock-sa" aria-label="Short answer">${esc(M.ans.sa)}</textarea></div>
    <div class="row"><button class="btn primary" data-act="submit">Submit exam</button><button class="btn ghost" data-act="abandon">Abandon this attempt</button></div>`;
  const tb = $("#timer");
  if (M.timed) {
    const tick = () => {
      const l = left();
      const c = $("#clock"); if (!c) { clearInterval(mockTimer); return; }
      c.textContent = fmt(l);
      tb.classList.toggle("low", l < 5 * 60000);
      if (l <= 0) { clearInterval(mockTimer); M.submitted = true; M.end = Date.now(); save(); renderMock(); window.scrollTo({ top: 0 }); }
    };
    mockTimer = setInterval(tick, 1000);
  }
  const upd = () => { const a = $("#answered"); if (a) a.textContent = answeredCount(); };
  view.oninput = ev => {
    const c = ev.target.closest("[data-code]");
    if (c) { M.ans.code[c.dataset.code] = c.value; save(); upd(); }
    if (ev.target.id === "mock-sa") { M.ans.sa = ev.target.value; save(); upd(); }
  };
  $$(".mock-code", view).forEach(t => t.addEventListener("keydown", editorKeys));
  view.onchange = null;
  view.onkeydown = null;
  view.onclick = ev => {
    const m = ev.target.closest("[data-mcq]");
    if (m) { M.ans.mcq[m.dataset.mcq] = +m.dataset.i; save(); $$(`[data-mcq="${m.dataset.mcq}"]`, view).forEach(b => b.classList.toggle("picked", b === m)); upd(); return; }
    const t = ev.target.closest("[data-tf]");
    if (t) { M.ans.tf[t.dataset.tf] = t.dataset.v === "true"; save(); $$(`[data-tf="${t.dataset.tf}"]`, view).forEach(b => b.classList.toggle("picked", b === t)); upd(); return; }
    const a = ev.target.closest("[data-act]"); if (!a) return;
    if (a.dataset.act === "submit") {
      const missing = 23 - answeredCount();
      $("#confirm").innerHTML = `<div class="note warn row spread fade-in"><span>${missing ? `${missing} question${missing > 1 ? "s are" : " is"} still blank. ` : ""}Submit and see your results?</span><span class="row"><button class="btn small primary" data-act="really-submit">Submit now</button><button class="btn small" data-act="keep">Keep working</button></span></div>`;
      $("#confirm").scrollIntoView({ behavior: "smooth", block: "center" });
    }
    if (a.dataset.act === "keep") $("#confirm").innerHTML = "";
    if (a.dataset.act === "really-submit") { clearInterval(mockTimer); M.submitted = true; M.end = Date.now(); save(); renderMock(); window.scrollTo({ top: 0 }); }
    if (a.dataset.act === "abandon") {
      if (a.dataset.confirm !== "1") { a.dataset.confirm = "1"; a.textContent = "Click again to delete this attempt"; return; }
      clearInterval(mockTimer); S.mockCur = null; save(); renderMock();
    }
  };
}
function renderMockStart() {
  clearHandlers();
  view.innerHTML = `
    <h1>Mock exam</h1>
    <section class="card stack">
      <p>A fresh paper in the real format, drawn from the question bank: <strong>10 MCQ, 10 true/false, 2 coding questions</strong> (one MapReduce / Hadoop Streaming, one PySpark / Spark SQL) and <strong>1 short answer</strong>. 50 minutes, like Friday.</p>
      <div class="blueprint" role="img" aria-label="Marks: multiple choice 20%, true/false 20%, code 40%, short answer 20%">
        <div class="mcq"><b>20%</b><span>10 MCQ</span></div><div class="tf"><b>20%</b><span>10 T/F</span></div><div class="code"><b>40%</b><span>2 coding</span></div><div class="sa"><b>20%</b><span>Short answer</span></div>
      </div>
      <p class="small muted">Your answers save as you go, so a refresh won't lose them. MCQ and T/F are marked automatically. For code and the short answer you mark yourself against the rubric${AI ? ", or ask Claude to grade them" : ""}.</p>
      <div class="row"><button class="btn primary" data-act="start-timed">Start 50-minute mock</button><button class="btn" data-act="start-untimed">Start without a timer</button></div>
    </section>
    ${S.mocks.length ? `<section class="card"><h2>Past attempts</h2><div class="table-wrap"><table><thead><tr><th>Date</th><th>MCQ</th><th>T/F</th><th>Code</th><th>Short</th><th>Total</th></tr></thead><tbody>${S.mocks.slice().reverse().map(x => `<tr><td>${esc(x.date)}</td><td>${esc(x.mcq)}/20</td><td>${esc(x.tf)}/20</td><td>${esc(x.code)}/40</td><td>${esc(x.sa)}/20</td><td><strong>${esc(x.total)}%</strong></td></tr>`).join("")}</tbody></table></div></section>` : ""}`;
  view.onclick = ev => {
    const a = ev.target.closest("[data-act]"); if (!a) return;
    if (a.dataset.act === "start-timed") { newMock(true); renderMock(); window.scrollTo({ top: 0 }); }
    if (a.dataset.act === "start-untimed") { newMock(false); renderMock(); window.scrollTo({ top: 0 }); }
  };
  view.oninput = null; view.onchange = null;
}
function mockScores(M) {
  const mcq = M.mcq.filter(it => M.ans.mcq[it.id] === QBYID[it.id].answer).length * 2;
  const tf = M.tf.filter(it => M.ans.tf[it.id] === QBYID[it.id].answer).length * 2;
  const codeScore = id => { const v = M.ai[id] != null ? M.ai[id] : M.self[id]; return v == null ? null : v; };
  const code = M.code.reduce((a, id) => a + (codeScore(id) || 0), 0);
  const sa = codeScore("sa") || 0;
  const pending = M.code.filter(id => codeScore(id) == null).length + (codeScore("sa") == null ? 1 : 0);
  return { mcq, tf, code, sa, total: mcq + tf + code + sa, pending };
}
function renderMockResults() {
  clearHandlers();
  const M = S.mockCur;
  const firstTime = !M.recorded;
  const mins = Math.round(((M.end || Date.now()) - M.start) / 60000);
  let n = 0;
  const review = (items, kind) => items.map(it => {
    const q = QBYID[it.id]; n++;
    const pick = kind === "mcq" ? M.ans.mcq[it.id] : M.ans.tf[it.id];
    const ok = pick === q.answer;
    const pickText = pick == null ? "no answer" : kind === "mcq" ? q.options[pick] : pick ? "True" : "False";
    if (firstTime && pick != null) record(q.id, ok);
    return `<li class="note ${ok ? "good" : "bad"}"><p><span class="qnum">Q${n}</span> <strong>${md(q.q)}</strong></p><p style="margin-top:4px">${ok ? "✓" : "✗"} You: ${md(pickText)}${ok ? "" : ` · Answer: <strong>${kind === "mcq" ? md(q.options[q.answer]) : q.answer ? "True" : "False"}</strong>`}</p>${ok ? "" : `<p class="small" style="margin-top:4px">${md(q.explain)}</p>`}</li>`;
  }).join("");
  const reviewHtml = review(M.mcq, "mcq") + review(M.tf, "tf");
  if (firstTime) { M.recorded = true; save(); }
  const sa = SHORT[M.sa];
  const saRubric = sa.points.map(p => ({ pts: 2, text: p }));
  const rubricFor = key => key === "sa" ? saRubric : CODE[key].rubric;
  const selfOf = key => {
    const rubric = rubricFor(key), ticks = (M.ticks && M.ticks[key]) || [];
    const max = rubric.reduce((a, r) => a + r.pts, 0);
    return Math.round(rubric.reduce((a, r, i) => a + (ticks[i] ? r.pts : 0), 0) * 20 / max);
  };
  const tagText = key => M.ai[key] != null ? `Claude: ${M.ai[key]}/20` : M.self[key] != null ? `You: ${M.self[key]}/20` : "not marked yet";
  const manual = (key, title, reference, answer, lang) => {
    const ticks = (M.ticks && M.ticks[key]) || [];
    return `<section class="card stack">
      <div class="row spread"><h3>${md(title)}</h3><span class="tag ${M.ai[key] != null || M.self[key] != null ? "good" : ""}" data-tag="${key}">${tagText(key)}</span></div>
      <p class="eyebrow">Your answer</p>${codeBlock(answer || "(blank)", lang)}
      <details><summary>Model answer</summary><div style="margin-top:8px">${lang === "text" ? `<div class="note">${md(reference)}</div>` : codeBlock(reference, lang)}</div></details>
      <div><p class="eyebrow">Mark yourself</p><ul class="clean rubric">${rubricFor(key).map((r, i) => `<li><input type="checkbox" id="mk-${key}-${i}" data-mk="${key}" data-i="${i}" ${ticks[i] ? "checked" : ""}><label for="mk-${key}-${i}">${md(r.text)}</label><span class="pts">${r.pts}</span></li>`).join("")}</ul></div>
      <div class="row"><button class="btn small accent ai-only" data-grade="${key}">Grade with Claude</button><span class="small muted">Self-mark: <strong data-selfout="${key}">${selfOf(key)}/20</strong></span></div>
      <div data-aiout="${key}" hidden></div>
    </section>`;
  };
  const summary = () => {
    const sc = mockScores(M);
    return {
      sc,
      total: `${sc.total}%`,
      note: `${sc.pending ? `Provisional: ${sc.pending} written answer${sc.pending > 1 ? "s" : ""} still to mark below.` : "All parts marked."} Time used: ${mins} min.`,
      grid: `<div>MCQ<b>${sc.mcq}/20</b></div><div>True/False<b>${sc.tf}/20</b></div><div>Code<b>${sc.code}/40</b></div><div>Short answer<b>${sc.sa}/20</b></div>`
    };
  };
  const sm = summary();
  view.innerHTML = `
    <h1>Mock exam results</h1>
    <section class="card stack">
      <div class="row"><div class="result-big" id="mock-total">${sm.total}</div><p id="mock-note">${sm.note}</p></div>
      <div class="score-grid" id="mock-grid">${sm.grid}</div>
      <div class="row"><button class="btn primary" data-act="save-mock">Save to history</button><button class="btn" data-act="new-mock">New mock exam</button></div>
    </section>
    ${M.code.map(id => { const ex = CODE[id]; return manual(id, ex.title, ex.solution, M.ans.code[id], ex.lang); }).join("")}
    ${manual("sa", "Short answer: " + sa.q, sa.model, M.ans.sa, "text")}
    <section class="card"><details><summary>Review Part A and B (${(sm.sc.mcq + sm.sc.tf) / 2}/20 right)</summary><ol class="clean stack" style="margin-top:10px">${reviewHtml}</ol></details></section>`;
  const refresh = key => {
    const x = summary();
    $("#mock-total").textContent = x.total;
    $("#mock-note").textContent = x.note;
    $("#mock-grid").innerHTML = x.grid;
    const tag = $(`[data-tag="${key}"]`);
    if (tag) { tag.textContent = tagText(key); tag.classList.add("good"); }
    const so = $(`[data-selfout="${key}"]`);
    if (so) so.textContent = selfOf(key) + "/20";
  };
  view.onchange = ev => {
    const k = ev.target.closest("[data-mk]");
    if (!k) return;
    const key = k.dataset.mk;
    M.ticks = M.ticks || {}; M.ticks[key] = M.ticks[key] || []; M.ticks[key][+k.dataset.i] = k.checked;
    M.self[key] = selfOf(key);
    save(); refresh(key);
  };
  view.onclick = ev => {
    const g = ev.target.closest("[data-grade]");
    if (g) {
      const key = g.dataset.grade;
      const isSa = key === "sa";
      const ex = isSa ? null : CODE[key];
      gradeWithClaude({
        question: isSa ? sa.q : ex.prompt + (ex.data ? "\nSample input:\n" + ex.data : ""),
        rubric: rubricFor(key), reference: isSa ? sa.model : ex.solution,
        langName: isSa ? "written short answer" : langName(ex),
        onScore: (score, outOf) => { M.ai[key] = Math.round(score * 20 / outOf); save(); refresh(key); }
      }, isSa ? M.ans.sa : (M.ans.code[key] || ""), $(`[data-aiout="${key}"]`), g);
      return;
    }
    const a = ev.target.closest("[data-act]"); if (!a) return;
    if (a.dataset.act === "save-mock") {
      const sc = mockScores(M);
      const d = new Date();
      S.mocks.push({ date: d.toLocaleDateString(undefined, { month: "short", day: "numeric" }) + " " + d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" }), mcq: sc.mcq, tf: sc.tf, code: sc.code, sa: sc.sa, total: sc.total });
      save(); a.textContent = "Saved ✓"; a.disabled = true;
    }
    if (a.dataset.act === "new-mock") { S.mockCur = null; save(); renderMock(); window.scrollTo({ top: 0 }); }
  };
}


/* ---------------- start ---------------- */
function setBarHeight() { document.documentElement.style.setProperty("--bar-h", $(".topbar").offsetHeight + "px"); }
window.addEventListener("resize", setBarHeight);
setBarHeight();
window.addEventListener("hashchange", () => {
  const h = (location.hash || "").slice(1);
  if (VIEWS.includes(h) && h !== state.view) go(h);
});
const initial = (location.hash || "").slice(1);
go(VIEWS.includes(initial) ? initial : "plan");
})();
