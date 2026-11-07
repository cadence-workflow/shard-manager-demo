// Renders snapshots streamed by the controller. Everything shown here comes
// from the server; the UI never decides or predicts shard placement.
"use strict";

const PALETTE = ["#58a6ff", "#3fb950", "#d29922", "#bc8cff", "#39c5cf", "#ff7b72", "#f778ba", "#a5d6ff"];
const HEAT_STOPS = [[47, 129, 247], [137, 87, 229], [219, 109, 40], [248, 81, 73]];
const MAX_SHARD_RPS = 450;
const MOVES_SHOWN = 14;

const $ = (id) => document.getElementById(id);
const workerColor = (i) => PALETTE[(i - 1) % PALETTE.length];

const shardEls = new Map();
const cardEls = new Map();
const lastOwner = new Map();
let mode = null;
let lastNarration = "";

// ---- streaming ----

function connect() {
  const es = new EventSource("/api/stream");
  es.onmessage = (e) => render(JSON.parse(e.data));
  es.onerror = () => { $("narration").textContent = "Reconnecting to the controller…"; };
}

// ---- actions ----

async function post(path, params = {}) {
  const res = await fetch(path, { method: "POST", body: new URLSearchParams(params) });
  if (!res.ok) toast(await res.text());
}

function toast(text) {
  const el = $("toast");
  el.textContent = text;
  el.classList.remove("hidden");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => el.classList.add("hidden"), 3500);
}

function setMode(next) {
  mode = mode === next ? null : next;
  document.body.classList.toggle("heat-mode", mode === "heat");
  document.body.classList.toggle("kill-mode", mode === "kill");
  $("heat-mode").classList.toggle("active", mode === "heat");
  $("kill-mode").classList.toggle("active", mode === "kill");
  $("mode-hint").textContent =
    mode === "heat" ? "Click a shard to make it 5× hotter (click a hot one to cool it)" :
    mode === "kill" ? "Click a worker to kill its process (SIGKILL)" : "";
}

$("heat-mode").onclick = () => setMode("heat");
$("kill-mode").onclick = () => setMode("kill");
$("add").onclick = () => post("/api/add");
$("reset").onclick = () => { setMode(null); post("/api/reset"); };
$("balancing").onclick = () => post("/api/balancing", { on: $("balancing").getAttribute("aria-pressed") !== "true" });

// ---- helpers ----

function heatColor(load) {
  const t = Math.min(1, Math.sqrt(Math.max(0, load) / MAX_SHARD_RPS)) * (HEAT_STOPS.length - 1);
  const i = Math.min(HEAT_STOPS.length - 2, Math.floor(t));
  const f = t - i;
  const [a, b] = [HEAT_STOPS[i], HEAT_STOPS[i + 1]];
  return `rgb(${a.map((v, k) => Math.round(v + (b[k] - v) * f)).join(",")})`;
}

const shardSize = (load) => 54 + 62 * Math.min(1, Math.sqrt(Math.max(0, load) / MAX_SHARD_RPS));

function timeOf(ms) {
  return new Date(ms).toLocaleTimeString([], { hour12: false });
}

function el(tag, cls, html) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (html !== undefined) e.innerHTML = html;
  return e;
}

function workerLabel(i) {
  return `<b style="color:${workerColor(i)}">Worker ${i}</b>`;
}

// ---- render ----

function render(s) {
  renderHeader(s);
  renderNarration(s.narration);
  renderBoard(s);
  renderMoves(s.moves || []);
  renderCPU(s.workers || []);
  renderEvents(s.events || []);

  $("overlay").classList.toggle("hidden", !s.busy);
  $("overlay-text").textContent = s.busy;
  $("idle").textContent = s.idleResetIn >= 0 && s.idleResetIn <= 30 ? `Auto-reset in ${s.idleResetIn}s` : "";
}

function renderHeader(s) {
  const meter = $("meter");
  $("imbalance").textContent = `${s.imbalance.toFixed(2)}×`;
  meter.className = "meter " + (s.imbalance <= 1.2 ? "good" : s.imbalance < 1.5 ? "warn" : "bad");

  const toggle = $("balancing");
  toggle.setAttribute("aria-pressed", String(s.balancing));
  toggle.classList.toggle("pending", s.balancingPendingMs > 0);
  $("balancing-state").textContent = `Load balancing ${s.balancing ? "ON" : "OFF"}`;
  $("balancing-hint").textContent = s.balancingPendingMs > 0
    ? `Shard Manager applies this within ~${Math.ceil(s.balancingPendingMs / 1000)}s`
    : s.balancing ? "Shard Manager balances by reported load" : "Click to turn on Shard Manager";
}

function renderNarration(text) {
  const n = $("narration");
  if (text === lastNarration) return;
  lastNarration = text;
  n.textContent = text;
  n.classList.remove("flash");
  void n.offsetWidth;
  n.classList.add("flash");
}

function ensureCard(w) {
  let card = cardEls.get(w.index);
  if (card) return card;
  card = el("div", "worker");
  card.style.setProperty("--c", workerColor(w.index));
  card.innerHTML = `
    <div class="worker-head">
      <span class="worker-name">Worker ${w.index}</span>
      <span class="worker-proc"></span>
      <button class="worker-kill">Kill</button>
    </div>
    <div class="worker-stats">
      <div class="stat"><strong class="s-shards">0</strong><span>shards</span></div>
      <div class="stat"><strong class="s-load">0</strong><span>reported RPS</span></div>
      <div class="stat"><strong class="s-cpu">0%</strong><span>CPU (OS)</span></div>
    </div>
    <div class="loadbar" title="Reported load; the white mark is the average"><i></i><b></b></div>
    <div class="shards"></div>
    <div class="status"></div>`;
  card.querySelector(".worker-kill").onclick = (e) => { e.stopPropagation(); post("/api/kill", { worker: w.index }); };
  card.onclick = () => {
    if (mode === "kill" && card.classList.contains("running")) { post("/api/kill", { worker: w.index }); setMode(null); }
  };
  $("workers").appendChild(card);
  cardEls.set(w.index, card);
  return card;
}

function ensureShard(id) {
  let e = shardEls.get(id);
  if (e) return e;
  e = el("div", "shard", `<b>${id}</b><small></small>`);
  e.dataset.shard = id;
  e.onclick = (ev) => {
    ev.stopPropagation();
    post("/api/heat", { shard: id });
    if (mode === "heat") setMode(null);
  };
  shardEls.set(id, e);
  return e;
}

const STATUS_TEXT = {
  dead: "Process killed.<br>Waiting for Shard Manager to notice (heartbeat TTL 2s)…",
  starting: "Starting… registering with Shard Manager",
  draining: "Draining…",
};

function renderBoard(s) {
  const workers = s.workers || [];
  const present = new Set(workers.map((w) => w.index));
  for (const [index, card] of cardEls) {
    if (!present.has(index)) { card.remove(); cardEls.delete(index); }
  }

  // FLIP: remember where every shard is before moving DOM nodes.
  const first = new Map();
  for (const [id, e] of shardEls) if (e.isConnected) first.set(id, e.getBoundingClientRect());

  const running = workers.filter((w) => w.state === "running");
  const maxLoad = Math.max(1, ...workers.map((w) => w.load));
  const meanLoad = running.length ? running.reduce((a, w) => a + w.load, 0) / running.length : 0;

  const shardById = new Map((s.shards || []).map((sh) => [sh.id, sh]));
  const flashed = new Set();

  for (const w of workers) {
    const card = ensureCard(w);
    card.classList.remove("running", "dead", "starting", "draining");
    card.classList.add(w.state);
    card.querySelector(".worker-proc").textContent = `${w.process} · pid ${w.pid}`;
    card.querySelector(".s-shards").textContent = w.shards.length;
    card.querySelector(".s-load").textContent = Math.round(w.load);
    card.querySelector(".s-cpu").textContent = `${Math.round(w.cpu)}%`;
    card.querySelector(".loadbar i").style.width = `${(100 * w.load) / maxLoad}%`;
    card.querySelector(".loadbar b").style.left = `${(100 * meanLoad) / maxLoad}%`;
    card.querySelector(".status").innerHTML = STATUS_TEXT[w.state] || "";
    const box = card.querySelector(".shards");
    for (const id of w.shards) box.appendChild(ensureShard(id));
  }

  const unassigned = (s.shards || []).filter((sh) => !sh.owner || !present.has(sh.owner));
  $("unassigned-wrap").classList.toggle("hidden", unassigned.length === 0);
  for (const sh of unassigned) $("unassigned").appendChild(ensureShard(sh.id));

  for (const sh of s.shards || []) {
    const e = ensureShard(sh.id);
    const size = shardSize(sh.load);
    e.style.setProperty("--size", `${size}px`);
    e.style.setProperty("--heat", heatColor(sh.load));
    e.querySelector("small").textContent = `${Math.round(sh.load)} rps`;
    e.classList.toggle("hot", sh.target > sh.base);
    e.title = `Shard ${sh.id}: target ${sh.target} RPS, reported ${sh.load} RPS`;

    const prev = lastOwner.get(sh.id);
    if (sh.owner && prev && prev !== sh.owner) {
      e.classList.remove("moved");
      void e.offsetWidth;
      e.classList.add("moved");
      clearTimeout(e.movedTimer);
      e.movedTimer = setTimeout(() => e.classList.remove("moved"), 4000);
      flashed.add(prev).add(sh.owner);
    }
    if (sh.owner) lastOwner.set(sh.id, sh.owner);
  }

  for (const index of flashed) {
    const card = cardEls.get(index);
    if (!card) continue;
    card.classList.remove("flash");
    void card.offsetWidth;
    card.classList.add("flash");
  }

  for (const [id, e] of shardEls) {
    const from = first.get(id);
    if (!from || !shardById.has(id)) continue;
    const to = e.getBoundingClientRect();
    const dx = from.left - to.left;
    const dy = from.top - to.top;
    if (Math.abs(dx) < 1 && Math.abs(dy) < 1) continue;
    const far = Math.hypot(dx, dy) > 120;
    e.animate(
      [{ transform: `translate(${dx}px, ${dy}px)`, zIndex: 5 }, { transform: "none", zIndex: 5 }],
      { duration: far ? 900 : 350, easing: "cubic-bezier(.2,.8,.2,1)" },
    );
  }
}

function renderMoves(moves) {
  const list = $("moves");
  const key = moves.slice(0, MOVES_SHOWN).map((m) => `${m.t}-${m.shard}`).join("|");
  if (list.dataset.key === key) return;
  list.dataset.key = key;
  list.innerHTML = "";
  if (!moves.length) { list.appendChild(el("li", "empty", "No moves yet")); return; }
  for (const m of moves.slice(0, MOVES_SHOWN)) {
    const li = el("li", "", `
      <span class="chip">Shard ${m.shard}</span>
      <span class="route">${workerLabel(m.from)} → ${workerLabel(m.to)}${m.load >= 1 ? `<small>${Math.round(m.load)} rps</small>` : ""}</span>
      <span class="reason ${m.reason.replace(" ", "-")}">${m.reason}</span>`);
    li.title = `Observed at ${timeOf(m.t)}`;
    li.onmouseenter = () => shardEls.get(m.shard)?.classList.add("highlight");
    li.onmouseleave = () => shardEls.get(m.shard)?.classList.remove("highlight");
    list.appendChild(li);
  }
}

function sparkline(values, scale, color) {
  const n = 60;
  const pts = values.slice(-n).map((v, i, arr) => {
    const x = ((n - arr.length + i) / (n - 1)) * 100;
    const y = 32 - (Math.min(v, scale) / scale) * 30;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const line = pts.join(" ");
  const area = pts.length ? `${pts[0].split(",")[0]},32 ${line} ${pts[pts.length - 1].split(",")[0]},32` : "";
  return `<svg viewBox="0 0 100 34" preserveAspectRatio="none">
    <polygon points="${area}" fill="${color}" opacity=".18"/>
    <polyline points="${line}" fill="none" stroke="${color}" stroke-width="1.6" vector-effect="non-scaling-stroke"/>
  </svg>`;
}

function renderCPU(workers) {
  const scale = Math.max(100, ...workers.flatMap((w) => w.cpuHistory || []));
  $("cpu").innerHTML = workers.map((w) => `
    <div class="cpu-row ${w.state === "dead" ? "dead" : ""}">
      <span class="name" style="color:${workerColor(w.index)}">${w.process}<small>pid ${w.pid}</small></span>
      ${sparkline(w.cpuHistory || [], scale, workerColor(w.index))}
      <span class="pct">${Math.round(w.cpu)}%</span>
    </div>`).join("") || `<span class="empty">No worker processes</span>`;
}

function renderEvents(events) {
  const list = $("events");
  const key = events.length ? `${events[0].t}-${events.length}` : "";
  if (list.dataset.key === key) return;
  list.dataset.key = key;
  list.innerHTML = events.map((e) => `<li class="ev-${e.kind}"><time>${timeOf(e.t)}</time>${e.text}</li>`).join("");
}

connect();
