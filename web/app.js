// Renders snapshots streamed by the controller. Everything shown here comes
// from the server; the UI never decides or predicts shard placement. Motion
// only replays what a snapshot just showed: a recorded heartbeat travels up
// its wire, and a shard whose owner changed slides to its new worker.
"use strict";

// Shard color goes from cool to hot by load, up to the heaviest a heated shard gets.
const MAX_SHARD_RPS = 180;
const HEAT_LIGHT = [[230, 233, 238], [252, 222, 140], [244, 140, 62], [198, 40, 40]];
const HEAT_DARK = [[44, 49, 58], [122, 92, 34], [196, 106, 40], [214, 64, 64]];
const PULSE_MS = 700;
const GLIDE_MS = 700;
const GLIDE_STAGGER_MS = 80;
const WIRE_DRAW_MS = 600;
const GONE_LINGER_MS = 1400;
const RING_RADIUS = 10;
const RING_LENGTH = 2 * Math.PI * RING_RADIUS;
const SVG_NS = "http://www.w3.org/2000/svg";

const $ = (id) => document.getElementById(id);
const darkMode = window.matchMedia("(prefers-color-scheme: dark)");

const cardEls = new Map();
const tileEls = new Map();
const lastOwner = new Map();

// Heartbeats: when Shard Manager last heard from each worker, by worker index.
const heartbeats = new Map();
const wireEls = new Map();
const pulses = [];
let heartbeatTTLMs = 4000;
let clockOffsetMs = 0;
let managerOn = true;
// Right after resuming, Shard Manager still catches up on heartbeats.
let catchingUp = false;

// ---- streaming and actions ----

function connect() {
  const es = new EventSource("/api/stream");
  es.onmessage = (e) => render(JSON.parse(e.data));
  es.onerror = () => { $("status").textContent = "Reconnecting to the demo controller…"; };
}

async function post(path, params = {}) {
  const res = await fetch(path, { method: "POST", body: new URLSearchParams(params) });
  if (!res.ok) toast(await res.text());
}

function toast(text) {
  const e = $("toast");
  e.textContent = text;
  e.classList.remove("hidden");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => e.classList.add("hidden"), 3500);
}

$("add").onclick = () => post("/api/add");
$("reset").onclick = () => post("/api/reset");
$("power").onclick = () => post("/api/manager", { on: $("power").getAttribute("aria-pressed") !== "true" });

const toggleHelp = (show = $("help").classList.contains("hidden")) => $("help").classList.toggle("hidden", !show);
$("help-open").onclick = () => toggleHelp(true);
$("help-close").onclick = () => toggleHelp(false);
$("help").onclick = (e) => { if (e.target === $("help")) toggleHelp(false); };
document.addEventListener("keydown", (e) => {
  if (e.key === "?") toggleHelp();
  else if (e.key === "Escape") toggleHelp(false);
});

// ---- render ----

function render(s) {
  trackHeartbeats(s);
  renderManager(s);
  setText($("status"), s.narration);
  renderWorkers(s);

  $("overlay").classList.toggle("hidden", !s.busy);
  $("overlay-text").textContent = s.busy;
  $("idle").textContent = s.idleResetIn >= 0 && s.idleResetIn <= 30 ? `Resets in ${s.idleResetIn}s` : "";
}

function renderManager(s) {
  managerOn = s.managerOn;
  catchingUp = s.managerOn && s.managerPendingMs > 0;
  const pending = s.managerOn && (s.managerPendingMs > 0 || s.managerTurningOff);
  const pendingText = s.managerTurningOff ? "Turning off…" : "Turning on…";
  const manager = $("manager");
  manager.classList.toggle("off", !s.managerOn);
  manager.classList.toggle("pending", pending);
  manager.classList.toggle("on", s.managerOn && !pending);
  manager.classList.toggle("down", s.managerOn && !s.smOK && !s.managerTurningOff);
  const workers = `${s.smExecutors} worker${s.smExecutors === 1 ? "" : "s"}`;
  setText($("manager-detail"), !s.managerOn ? "Shards are placed by count only"
    : s.managerTurningOff ? "Load balancing disabled · placing by count"
    : s.smOK ? `${workers} connected · heartbeat TTL ${heartbeatTTLMs / 1000}s` : "Unreachable");
  setText($("mode"), !s.managerOn ? "Off" : pending ? pendingText : "On");
  document.querySelectorAll(".help .ttl").forEach((e) => { e.textContent = heartbeatTTLMs / 1000; });

  const power = $("power");
  power.setAttribute("aria-pressed", String(s.managerOn && !s.managerTurningOff));
  power.classList.toggle("pending", pending);
  setText($("power-hint"), !s.managerOn ? "Off" : pending ? pendingText : "On");

  const measurable = !s.managerTurningOff &&
    (s.workers || []).filter((w) => w.state === "running" && w.shards.length > 0).length >= 2;
  setText($("imbalance-value"), !measurable ? "–" : s.imbalance >= 10 ? "10×+" : `${s.imbalance.toFixed(1)}×`);
  $("imbalance").className = "imbalance " + (!measurable ? "" : s.imbalance <= 1.3 ? "good" : s.imbalance < 2 ? "warn" : "bad");
}

const NOTES = {
  starting: "Starting…",
  draining: "Draining…",
};

function renderWorkers(s) {
  const workers = s.workers || [];
  const present = new Set(workers.map((w) => w.index));

  // Remember where cards and shards are before touching the DOM, to animate.
  const cardsBefore = new Map();
  for (const [index, card] of cardEls) cardsBefore.set(index, card.getBoundingClientRect());
  const slot = $("add");
  const slotBefore = slot.hidden ? null : slot.getBoundingClientRect();
  const before = new Map();
  for (const [id, e] of tileEls) if (e.isConnected) before.set(id, e.getBoundingClientRect());
  // A worker Shard Manager forgot stays a moment, so its shards visibly leave it.
  for (const [index, card] of cardEls) {
    if (!present.has(index) && !card.dataset.gone) {
      card.dataset.gone = "1";
      card.classList.add("gone");
      setText(card.querySelector(".note"), "Declared dead");
      setTimeout(() => flipCards(() => { card.remove(); cardEls.delete(index); }), GONE_LINGER_MS);
    }
  }

  const running = workers.filter((w) => w.state === "running");
  const maxLoad = Math.max(1, ...workers.map((w) => w.load));
  const meanLoad = running.length ? running.reduce((sum, w) => sum + w.load, 0) / running.length : 0;

  const created = [];
  for (const w of workers) {
    if (!cardEls.has(w.index)) created.push(w.index);
    const card = ensureCard(w.index);
    card.classList.remove("running", "dead", "starting", "draining");
    card.classList.add(w.state);
    card.querySelector(".load-bar i").style.width = `${(100 * w.load) / maxLoad}%`;
    card.querySelector(".load-bar b").style.left = `${(100 * meanLoad) / maxLoad}%`;
    setText(card.querySelector(".load span"), `${Math.round(w.load)} rps`);
    setText(card.querySelector(".cpu"), `${Math.round(w.cpu)}% CPU · pid ${w.pid}`);
    if (w.state !== "dead") setText(card.querySelector(".note"), NOTES[w.state] || "");
    const tiles = card.querySelector(".tiles");
    for (const id of w.shards) tiles.appendChild(ensureTile(id));
  }

  const alive = workers.filter((w) => w.state !== "dead").length;
  slot.hidden = Boolean(s.busy) || s.managerTurningOff || alive >= s.maxWorkers;

  const unassigned = (s.shards || []).filter((sh) => !sh.owner || !present.has(sh.owner));
  $("unassigned-wrap").classList.toggle("hidden", unassigned.length === 0);
  for (const sh of unassigned) $("unassigned").appendChild(ensureTile(sh.id));

  const moved = new Map();
  for (const sh of s.shards || []) {
    const tile = ensureTile(sh.id);
    paintTile(tile, sh.load);
    setText(tile, String(Math.round(sh.load)));
    tile.classList.toggle("hot", sh.target > sh.base);
    tile.title = `Shard ${sh.id}: ${Math.round(sh.load)} requests/sec${sh.target > sh.base ? " (heated up)" : ""}`;

    const previous = lastOwner.get(sh.id);
    if (sh.owner && previous && previous !== sh.owner) moved.set(sh.id, { from: previous, to: sh.owner });
    if (sh.owner) lastOwner.set(sh.id, sh.owner);
  }

  // Cards slide when one joins or leaves; their shards ride along.
  const cardDelta = new Map();
  for (const [index, card] of cardEls) {
    const from = cardsBefore.get(index);
    if (!from || card.dataset.gone) continue;
    const to = card.getBoundingClientRect();
    const d = [from.left - to.left, from.top - to.top];
    if (Math.abs(d[0]) < 1 && Math.abs(d[1]) < 1) continue;
    cardDelta.set(index, d);
    card.animate([{ transform: `translate(${d[0]}px, ${d[1]}px)` }, { transform: "none" }],
      { duration: 500, easing: "cubic-bezier(.2, .8, .2, 1)" });
  }
  // A single new worker grows out of the slot it was added from.
  if (created.length === 1 && slotBefore) growFromSlot(cardEls.get(created[0]), slotBefore);
  else for (const index of created) fadeIn(cardEls.get(index));
  if (slotBefore && cardsBefore.size > 0 && !slot.hidden) slide(slot, slotBefore);

  let glides = 0;
  for (const [id, tile] of tileEls) {
    const from = before.get(id);
    if (!from) continue;
    if (moved.has(id)) {
      if (glides === 0 && managerOn) flashManager();
      glide(tile, from, glides++ * GLIDE_STAGGER_MS);
      continue;
    }
    const to = tile.getBoundingClientRect();
    const owner = lastOwner.get(id);
    const [cdx, cdy] = cardDelta.get(owner) || [0, 0];
    const dx = from.left - to.left - cdx;
    const dy = from.top - to.top - cdy;
    if (Math.abs(dx) < 1 && Math.abs(dy) < 1) continue;
    tile.animate(
      [{ transform: `translate(${dx}px, ${dy}px)`, zIndex: 5 }, { transform: "none", zIndex: 5 }],
      { duration: 300, easing: "cubic-bezier(.2, .8, .2, 1)" },
    );
  }
}

// flipCards slides the remaining cards and the add slot after the row changes.
function flipCards(change) {
  const items = [...cardEls.values(), $("add")];
  const rects = new Map(items.map((e) => [e, e.getBoundingClientRect()]));
  change();
  for (const e of items) if (e.isConnected) slide(e, rects.get(e));
}

function slide(e, from) {
  const to = e.getBoundingClientRect();
  const dx = from.left - to.left;
  const dy = from.top - to.top;
  if (Math.abs(dx) < 1 && Math.abs(dy) < 1) return;
  e.animate([{ transform: `translate(${dx}px, ${dy}px)` }, { transform: "none" }],
    { duration: 500, easing: "cubic-bezier(.2, .8, .2, 1)" });
}

function fadeIn(card) {
  card.animate([{ opacity: 0, transform: "translateY(16px)" }, { opacity: 1, transform: "none" }],
    { duration: 450, easing: "cubic-bezier(.2, .8, .2, 1)" });
}

// growFromSlot starts the card at the slot's place and size and unfolds it.
function growFromSlot(card, from) {
  const to = card.getBoundingClientRect();
  const inset = `inset(0 ${Math.max(0, to.width - from.width)}px ${Math.max(0, to.height - from.height)}px 0 round 10px)`;
  card.animate([
    { transform: `translate(${from.left - to.left}px, ${from.top - to.top}px)`, clipPath: inset },
    { transform: "none", clipPath: "inset(0 0 0 0 round 10px)" },
  ], { duration: 500, easing: "cubic-bezier(.2, .8, .2, 1)" });
}

// glide moves a shard that changed owner straight across to its new worker.
// Only ownership changes hands through Shard Manager, never the shard itself.
// The real tile waits, hidden, in its new place.
function glide(tile, fromRect, delay) {
  tile.flight?.cancel();
  const box = $("topology").getBoundingClientRect();
  const to = tile.getBoundingClientRect();
  const at = (r) => `translate(${r.left - box.left}px, ${r.top - box.top}px)`;

  const ghost = tile.cloneNode(true);
  ghost.classList.add("flying");
  ghost.classList.remove("moved");
  ghost.style.width = `${fromRect.width}px`;
  ghost.style.height = `${fromRect.height}px`;
  $("topology").appendChild(ghost);
  tile.style.visibility = "hidden";

  const anim = ghost.animate([{ transform: at(fromRect) }, { transform: at(to) }],
    { duration: GLIDE_MS, delay, easing: "cubic-bezier(.4, 0, .2, 1)", fill: "both" });
  const land = () => {
    ghost.remove();
    tile.style.visibility = "";
    tile.flight = null;
  };
  tile.flight = { cancel: () => { anim.cancel(); land(); } };
  anim.onfinish = () => {
    land();
    tile.classList.add("moved");
    clearTimeout(tile.movedTimer);
    tile.movedTimer = setTimeout(() => tile.classList.remove("moved"), 2000);
  };
}

function flashManager() {
  const manager = $("manager");
  manager.classList.add("deciding");
  clearTimeout(flashManager.timer);
  flashManager.timer = setTimeout(() => manager.classList.remove("deciding"), 350);
}

function ensureCard(index) {
  let card = cardEls.get(index);
  if (card) return card;
  card = document.createElement("div");
  card.className = "worker";
  card.innerHTML = `
    <div class="ring" title="Beats when Shard Manager records a heartbeat, and fills up while it waits for the next one">
      <svg viewBox="0 0 28 28"><circle class="track" cx="14" cy="14" r="${RING_RADIUS}"/><circle class="age" cx="14" cy="14" r="${RING_RADIUS}"/></svg>
      <svg class="heart" viewBox="0 0 16 16"><path d="M8 14s-5.6-3.5-5.6-7.4A3 3 0 0 1 8 4.7a3 3 0 0 1 5.6 1.9C13.6 10.5 8 14 8 14z"/></svg>
      <svg class="heart broken" viewBox="0 0 16 16">
        <path class="half" d="M8 14s-5.6-3.5-5.6-7.4A3 3 0 0 1 8 4.7L7 7.2l2 2-1.5 2z"/>
        <path class="half" d="M8 4.7a3 3 0 0 1 5.6 1.9C13.6 10.5 8 14 8 14l-.5-2.8 1.5-2-2-2z"/>
      </svg>
    </div>
    <div class="worker-head">
      <span class="worker-name">Worker ${index}</span>
      <span class="worker-heartbeat"></span>
    </div>
    <div class="load" title="Load the worker reports to Shard Manager. The blue mark is the average."><div class="load-bar"><i></i><b></b></div><span></span></div>
    <div class="tiles"></div>
    <div class="worker-foot"><span class="cpu"></span><button class="kill">Kill</button></div>
    <div class="note"></div>`;
  card.querySelector(".kill").onclick = () => post("/api/kill", { worker: index });
  $("workers").insertBefore(card, $("add"));
  cardEls.set(index, card);
  return card;
}

function ensureTile(id) {
  let tile = tileEls.get(id);
  if (tile) return tile;
  tile = document.createElement("div");
  tile.className = "tile";
  tile.onclick = () => post("/api/heat", { shard: id });
  tileEls.set(id, tile);
  return tile;
}

function paintTile(tile, load) {
  const stops = darkMode.matches ? HEAT_DARK : HEAT_LIGHT;
  const t = Math.min(1, Math.sqrt(Math.max(0, load) / MAX_SHARD_RPS));
  const scaled = t * (stops.length - 1);
  const i = Math.min(stops.length - 2, Math.floor(scaled));
  const f = scaled - i;
  const rgb = stops[i].map((v, k) => Math.round(v + (stops[i + 1][k] - v) * f));
  tile.style.setProperty("--heat", `rgb(${rgb.join(",")})`);
  tile.style.setProperty("--ink", t > 0.62 ? "#fff" : "var(--text)");
}

function setText(e, text) {
  if (e.textContent !== text) e.textContent = text;
}

// ---- heartbeats ----
// Snapshots say when Shard Manager last heard from each worker. Between
// snapshots the time since then keeps growing here, so the rings and countdowns
// move smoothly. When a newer heartbeat shows up, the ring beats and a dot
// travels up the wire into Shard Manager.

function trackHeartbeats(s) {
  heartbeatTTLMs = s.heartbeatTTLMs || heartbeatTTLMs;
  clockOffsetMs = s.now - Date.now();
  const present = new Set();
  for (const w of s.workers || []) {
    present.add(w.index);
    const previous = heartbeats.get(w.index);
    // With Shard Manager off, nobody is shown listening for heartbeats.
    if (s.managerOn && previous && previous.last && w.lastHeartbeat > previous.last) {
      pulses.push({ index: w.index, start: performance.now() });
      beatRing(w.index);
    }
    if (s.managerOn && previous && previous.state !== "dead" && w.state === "dead") breakHeart(w.index);
    heartbeats.set(w.index, { last: w.lastHeartbeat, state: w.state });
  }
  for (const index of heartbeats.keys()) if (!present.has(index)) heartbeats.delete(index);
}

// beatRing shows a heart in a worker's ring that goes lub-dub and fades out.
function beatRing(index) {
  const heart = cardEls.get(index)?.querySelector(".ring .heart");
  heart?.animate(
    [
      { transform: "scale(.6)", opacity: 0 },
      { transform: "scale(1.3)", opacity: 1, offset: 0.12 },
      { transform: "scale(1)", offset: 0.26 },
      { transform: "scale(1.18)", offset: 0.38 },
      { transform: "scale(1)", opacity: 1, offset: 0.6 },
      { transform: "scale(1)", opacity: 0 },
    ],
    { duration: 900, easing: "ease-out" },
  );
}

// breakHeart shows a heart that cracks in two and fades, when a worker dies.
function breakHeart(index) {
  const broken = cardEls.get(index)?.querySelector(".ring .broken");
  if (!broken) return;
  const timing = { duration: 1500, easing: "ease-out" };
  broken.animate(
    [
      { transform: "scale(.6)", opacity: 0 },
      { transform: "scale(1.2)", opacity: 1, offset: 0.12 },
      { transform: "scale(1)", opacity: 1, offset: 0.25 },
      { transform: "scale(1)", opacity: 1, offset: 0.7 },
      { transform: "scale(1)", opacity: 0 },
    ],
    timing,
  );
  broken.querySelectorAll(".half").forEach((half, i) => {
    const side = i === 0 ? -1 : 1;
    half.animate(
      [
        { transform: "none" },
        { transform: "none", offset: 0.3 },
        { transform: `translateX(${side * 0.8}px) rotate(${side * 11}deg)`, offset: 0.5 },
        { transform: `translateX(${side * 0.8}px) rotate(${side * 11}deg)` },
      ],
      timing,
    );
  });
}

// heartbeatAge is the time since Shard Manager last heard from the worker, or
// null if Shard Manager does not know it.
function heartbeatAge(index) {
  const hb = heartbeats.get(index);
  if (!hb || !hb.last) return null;
  return Math.max(0, Date.now() + clockOffsetMs - hb.last);
}

// wireCurves returns, per card, the bezier from its port on the bottom of the
// Shard Manager box down to the card's heartbeat ring, relative to the
// topology box. Ports are spread along the box in the order of the cards.
function wireCurves() {
  const box = $("topology").getBoundingClientRect();
  const manager = $("manager").getBoundingClientRect();
  const rings = [...cardEls].map(([index, card]) => [index, card.querySelector(".ring").getBoundingClientRect()]);
  rings.sort((a, b) => a[1].left - b[1].left);
  const curves = new Map();
  rings.forEach(([index, ring], i) => {
    const start = [manager.left + (manager.width * (i + 1)) / (rings.length + 1) - box.left, manager.bottom - box.top];
    const end = [ring.left + ring.width / 2 - box.left, ring.top - box.top];
    const bend = (end[1] - start[1]) / 2;
    curves.set(index, [start, [start[0], start[1] + bend], [end[0], end[1] - bend], end]);
  });
  return curves;
}

function pointOnCurve([p0, p1, p2, p3], t) {
  const u = 1 - t;
  return [0, 1].map((k) => u * u * u * p0[k] + 3 * u * u * t * p1[k] + 3 * u * t * t * p2[k] + t * t * t * p3[k]);
}

const easeInOut = (t) => (t < 0.5 ? 2 * t * t : 1 - (-2 * t + 2) ** 2 / 2);
const seconds = (ms) => (ms / 1000).toFixed(1);

function ensureWire(index) {
  let wire = wireEls.get(index);
  if (wire) return wire;
  const path = document.createElementNS(SVG_NS, "path");
  path.setAttribute("class", "wire");
  const port = document.createElementNS(SVG_NS, "circle");
  port.setAttribute("class", "port");
  port.setAttribute("r", "5");
  const label = document.createElement("div");
  label.className = "wire-label hidden";
  $("wires").prepend(path);
  $("wires").appendChild(port);
  $("topology").appendChild(label);
  wire = { path, port, label, drawnAt: performance.now() };
  wireEls.set(index, wire);
  return wire;
}

function renderHeartbeats(now) {
  const curves = wireCurves();
  for (const [index, card] of cardEls) {
    const curve = curves.get(index);
    const wire = ensureWire(index);
    const [a, b, c, d] = curve;
    wire.path.setAttribute("d", `M${a} C${b} ${c} ${d}`);
    wire.port.setAttribute("cx", a[0]);
    wire.port.setAttribute("cy", a[1]);

    // A new wire draws itself from Shard Manager down to the worker.
    const drawn = (now - wire.drawnAt) / WIRE_DRAW_MS;
    if (drawn < 1) {
      const length = wire.path.getTotalLength();
      wire.path.style.strokeDasharray = `${length}`;
      wire.path.style.strokeDashoffset = `${length * (1 - easeInOut(Math.max(0, drawn)))}`;
    } else if (wire.path.style.strokeDasharray) {
      wire.path.style.strokeDasharray = wire.path.style.strokeDashoffset = "";
    }

    // A paused Shard Manager hears nothing, so heartbeat ages mean nothing
    // until it answers again.
    const listening = managerOn && !catchingUp;
    const gone = Boolean(card.dataset.gone);
    const age = gone || !listening ? null : heartbeatAge(index);
    const dead = gone || heartbeats.get(index)?.state === "dead";
    const late = !dead && age !== null && age > heartbeatTTLMs * 0.6;
    wire.path.classList.toggle("off", !managerOn);
    wire.path.classList.toggle("silent", managerOn && !dead && age === null);
    wire.path.classList.toggle("dead", dead);
    wire.port.classList.toggle("dead", dead);

    const ring = card.querySelector(".ring");
    const fill = gone ? 1 : age === null ? 0 : Math.min(1, age / heartbeatTTLMs);
    ring.querySelector(".age").setAttribute("stroke-dasharray", `${fill * RING_LENGTH} ${RING_LENGTH}`);
    ring.classList.toggle("empty", fill === 0);
    ring.classList.toggle("late", late);
    ring.classList.toggle("expired", dead);

    // Only a late heartbeat is worth words; a healthy ring says enough.
    const heartbeatText = card.querySelector(".worker-heartbeat");
    setText(heartbeatText, dead && !gone ? "Killed" : late ? `no heartbeat for ${seconds(age)}s` : "");
    heartbeatText.classList.toggle("expired", dead);

    // A dead worker's countdown sits on its wire, while Shard Manager is listening.
    wire.label.classList.toggle("hidden", !dead || (!listening && !gone));
    if (dead) {
      const [x, y] = pointOnCurve(curve, 0.5);
      wire.label.style.left = `${x}px`;
      wire.label.style.top = `${y}px`;
      let text = "declared dead";
      if (!gone && age !== null) {
        text = age <= heartbeatTTLMs ? `silent ${seconds(age)}s / ${seconds(heartbeatTTLMs)}s` : "TTL passed";
      }
      setText(wire.label, text);
    }
  }

  for (const [index, wire] of wireEls) {
    if (!cardEls.has(index)) {
      wire.path.remove();
      wire.port.remove();
      wire.label.remove();
      wireEls.delete(index);
    }
  }

  for (let i = pulses.length - 1; i >= 0; i--) {
    const pulse = pulses[i];
    const curve = curves.get(pulse.index);
    const t = (now - pulse.start) / PULSE_MS;
    if (!curve || t >= 1) {
      pulse.el?.remove();
      pulses.splice(i, 1);
      if (curve) hitPort(wireEls.get(pulse.index)?.port);
      continue;
    }
    if (!pulse.el) {
      pulse.el = document.createElementNS(SVG_NS, "circle");
      pulse.el.setAttribute("class", "pulse");
      pulse.el.setAttribute("r", "4.5");
      $("wires").appendChild(pulse.el);
    }
    // Heartbeats travel from the worker up to Shard Manager.
    const [x, y] = pointOnCurve(curve, 1 - easeInOut(t));
    pulse.el.setAttribute("cx", x);
    pulse.el.setAttribute("cy", y);
  }

  requestAnimationFrame(renderHeartbeats);
}

// hitPort lights up a port on the Shard Manager box when a heartbeat lands.
function hitPort(port) {
  if (!port) return;
  port.classList.remove("hit");
  void port.getBBox();
  port.classList.add("hit");
}

requestAnimationFrame(renderHeartbeats);
connect();
