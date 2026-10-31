"use strict";

/* Kafka Flow Viewer UI: draws producers -> topics -> consumer groups from server-sent snapshots. */

const nodeW = 236;
const NODE_H = 50;
const ROW = 62;
const PART_ROW = 17;                // one partition line inside an expanded node
const PART_MAX = 24;                // more partitions than this: show the busiest
const HISTORY = 150;               // samples kept per node (5 min at the default 2 s interval)
const SVG_NS = "http://www.w3.org/2000/svg";

const els = {
  svg: document.getElementById("graph"),
  canvas: document.getElementById("canvas"),
  heads: document.getElementById("column-heads"),
  empty: document.getElementById("empty"),
  detail: document.getElementById("detail"),
  banner: document.getElementById("banner"),
  name: document.getElementById("cluster-name"),
  meta: document.getElementById("cluster-meta"),
  totalIn: document.getElementById("total-in"),
  totalOut: document.getElementById("total-out"),
  totalLag: document.getElementById("total-lag"),
  filter: document.getElementById("filter"),
  hideIdle: document.getElementById("hide-idle"),
  live: document.getElementById("live"),
  liveText: document.getElementById("live-text"),
};

const state = {
  snap: null,
  nodes: new Map(),      // id -> node data (with kind) from the latest snapshot
  history: new Map(),    // id -> [{t, rate, lag}]
  selected: null,
  hovered: null,
  lastMessageAt: 0,
  animated: [],          // [{el, speed, id}]
  offsets: new Map(),    // link id -> current dash offset, survives re-renders
  parts: null,           // {node, rows} per-partition rates of the selected node, from /api/partitions
};

const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

// ---------- formatting ----------

function fmtNum(n) {
  if (n == null || isNaN(n)) return "–";
  const a = Math.abs(n);
  if (a === 0) return "0";
  if (a < 10) return n.toFixed(1).replace(/\.0$/, "");
  if (a < 1000) return Math.round(n).toString();
  if (a < 1e6) return (n / 1e3).toFixed(a < 1e4 ? 1 : 0).replace(/\.0$/, "") + "k";
  if (a < 1e9) return (n / 1e6).toFixed(a < 1e7 ? 1 : 0).replace(/\.0$/, "") + "M";
  return (n / 1e9).toFixed(1).replace(/\.0$/, "") + "B";
}
const fmtRate = (r) => fmtNum(r) + "/s";

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

/** Characters that fit in a node after reserving `reserved` px for padding and side labels. */
const fit = (reserved) => Math.max(8, Math.floor((nodeW - reserved) / 7.6));

function clip(s, max) {
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}

function ago(ms) {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 5) return "just now";
  if (s < 90) return s + " s ago";
  return Math.round(s / 60) + " min ago";
}

// ---------- history & lag trend ----------

function remember(id, rate, lag, t) {
  let h = state.history.get(id);
  if (!h) state.history.set(id, (h = []));
  h.push({ t, rate, lag });
  if (h.length > HISTORY) h.shift();
}

/** Lag is "rising" when its trend over the last ~10 samples clearly goes up (least-squares slope). */
function lagRising(id) {
  const h = state.history.get(id);
  if (!h || h.length < 5) return false;
  const pts = h.slice(-10).map((p) => p.lag ?? 0);
  const n = pts.length, mx = (n - 1) / 2, my = pts.reduce((a, b) => a + b, 0) / n;
  let num = 0, den = 0;
  pts.forEach((y, x) => { num += (x - mx) * (y - my); den += (x - mx) ** 2; });
  const growth = (num / den) * (n - 1);          // lag gained over the window according to the trend
  return growth > Math.max(100, my * 0.15);
}

// ---------- data intake ----------

function ingest(snap) {
  state.snap = snap;
  state.lastMessageAt = Date.now();
  state.nodes.clear();
  const t = snap.timestamp;
  if (!snap.error) {
    for (const p of snap.producers) remember(p.id, p.rate, 0, t);
    for (const x of snap.topics) remember(x.id, x.inRate, x.lag, t);
    for (const g of snap.groups) remember(g.id, g.rate, g.lag, t);
    for (const l of snap.links) if (l.kind === "consume") remember(l.id, l.rate, l.lag, t);
    remember("totals", snap.totals.inRate, snap.totals.lag, t);
  }
  for (const p of snap.producers) state.nodes.set(p.id, { ...p, kind: "producer" });
  for (const x of snap.topics) state.nodes.set(x.id, { ...x, kind: "topic" });
  for (const g of snap.groups) state.nodes.set(g.id, { ...g, kind: "group" });
  if (state.selected && !state.nodes.has(state.selected)) { state.selected = null; state.parts = null; }
  renderHeader();
  render();
  renderDetail();
  if (state.selected && !snap.error) fetchPartitions(state.selected);
}

// ---------- header ----------

function renderHeader() {
  const s = state.snap;
  els.name.textContent = s.source || "Kafka cluster";
  const parts = [];
  if (s.clusterId) parts.push("Cluster " + s.clusterId);
  if (s.brokers) parts.push(s.brokers + (s.brokers === 1 ? " broker" : " brokers"));
  parts.push(s.totals.topics + " topics", s.totals.groups + " groups", s.totals.producers + " producers");
  els.meta.textContent = s.warmingUp && !s.error ? "Measuring rates for the first time…" : parts.join(", ");

  els.totalIn.innerHTML = fmtNum(s.totals.inRate) + "<small>msg/s</small>";
  els.totalOut.innerHTML = fmtNum(s.totals.outRate) + "<small>msg/s</small>";
  els.totalLag.innerHTML = fmtNum(s.totals.lag) + "<small>msgs</small>";
  els.totalLag.classList.toggle("rising", lagRising("totals"));

  if (s.error) {
    els.banner.className = "banner";
    els.banner.innerHTML = s.topics.length
      ? `<strong>Cluster unreachable.</strong> ${esc(s.error)}. Showing the last known state; retrying every ${Math.round(s.pollIntervalMs / 1000)} s.`
      : `<strong>Cannot reach the cluster.</strong> ${esc(s.error)}. Retrying every ${Math.round(s.pollIntervalMs / 1000)} s.`;
    els.banner.hidden = false;
  } else if (s.warnings && s.warnings.length) {
    els.banner.className = "banner warn";
    els.banner.textContent = s.warnings.join(". ") + ".";
    els.banner.hidden = false;
  } else {
    els.banner.hidden = true;
  }
}

// ---------- visibility (filter + hide idle) ----------

function visibleGraph() {
  const s = state.snap;
  const q = els.filter.value.trim().toLowerCase();
  const hideIdle = els.hideIdle.checked;
  const label = (n) => (n.name || n.groupId || n.label || "").toLowerCase();

  const linksByNode = new Map();
  for (const l of s.links) {
    for (const end of [l.source, l.target]) {
      if (!linksByNode.has(end)) linksByNode.set(end, []);
      linksByNode.get(end).push(l);
    }
  }

  let topics = s.topics.map((t) => state.nodes.get(t.id)).filter((t) => !(hideIdle && t.inRate === 0 && t.outRate === 0));
  if (q) {
    const matchingOthers = new Set(
      [...s.producers, ...s.groups].filter((n) => label(n).includes(q)).map((n) => n.id));
    topics = topics.filter((t) =>
      label(t).includes(q) || (linksByNode.get(t.id) || []).some((l) => matchingOthers.has(l.source) || matchingOthers.has(l.target)));
  }
  const topicIds = new Set(topics.map((t) => t.id));
  const links = s.links.filter((l) => topicIds.has(l.kind === "produce" ? l.target : l.source));
  const linked = new Set(links.flatMap((l) => [l.source, l.target]));

  const keep = (n) => {
    if (linked.has(n.id)) return !(q && hideIdle && n.rate === 0 && !(n.lag > 0));
    if (hideIdle || n.kind === "producer") return false;
    return !q || label(n).includes(q);            // e.g. empty consumer groups with no offsets
  };
  const producers = s.producers.map((p) => state.nodes.get(p.id)).filter(keep);
  const groups = s.groups.map((g) => state.nodes.get(g.id)).filter(keep);
  return { topics, producers, groups, links };
}

// ---------- layout ----------
//
// Layered left-to-right layout: every link points to a column further right. Plain flows take three columns
// (producers, topics, consumer groups); a group that also writes pushes its output topics one column further,
// so a chain p1 -> topic -> c1 -> topic -> c2 reads left to right.

function columnsOf(g) {
  const nodes = [...g.producers, ...g.topics, ...g.groups];
  const ids = new Set(nodes.map((n) => n.id));
  const succ = new Map(nodes.map((n) => [n.id, []]));
  const pred = new Map(nodes.map((n) => [n.id, []]));
  const edges = g.links.filter((l) => ids.has(l.source) && ids.has(l.target) && l.source !== l.target);

  // Drop the edges that close a cycle (a group writing back to a topic upstream of itself) before layering.
  const back = new Set();
  const stateOf = new Map();
  const out = new Map(nodes.map((n) => [n.id, []]));
  for (const l of edges) out.get(l.source).push(l);
  const visit = (id) => {
    stateOf.set(id, 1);
    for (const l of out.get(id)) {
      const st = stateOf.get(l.target);
      if (st === 1) back.add(l);
      else if (!st) visit(l.target);
    }
    stateOf.set(id, 2);
  };
  for (const n of nodes) if (!stateOf.get(n.id)) visit(n.id);
  const dag = edges.filter((l) => !back.has(l));
  for (const l of dag) { succ.get(l.source).push(l.target); pred.get(l.target).push(l.source); }

  const base = { producer: 0, topic: 1, group: 2 };
  const layer = new Map(nodes.map((n) => [n.id, base[n.kind]]));
  for (let changed = true, guard = 0; changed && guard <= nodes.length; guard++) {
    changed = false;
    for (const l of dag) {
      if (layer.get(l.target) <= layer.get(l.source)) { layer.set(l.target, layer.get(l.source) + 1); changed = true; }
    }
  }
  // Sources sit just left of what they feed, instead of all the way left.
  const bySourceDepth = nodes.filter((n) => !pred.get(n.id).length && succ.get(n.id).length)
    .sort((x, y) => layer.get(y.id) - layer.get(x.id));
  for (const n of bySourceDepth) {
    layer.set(n.id, Math.max(layer.get(n.id), Math.min(...succ.get(n.id).map((t) => layer.get(t))) - 1));
  }

  const used = [...new Set(layer.values())].sort((x, y) => x - y);
  const cols = used.map(() => []);
  for (const n of nodes) cols[used.indexOf(layer.get(n.id))].push(n);
  return { cols, succ, pred };
}

// ---------- expanded node: per-partition rows ----------

/** Lines drawn inside the selected node: section/topic headings and partition rows. */
function partitionLines(n) {
  if (state.selected !== n.id) return null;
  const parts = state.parts && state.parts.node === n.id ? state.parts.rows : null;
  if (!parts) return [{ type: "note", text: "Loading partitions…" }];
  if (!parts.length) {
    return [{ type: "note", text: n.kind === "group" ? "No committed offsets yet." : "No partition data." }];
  }
  let rows = parts;
  let hidden = 0;
  if (rows.length > PART_MAX) {
    const keep = new Set([...rows].sort((a, b) => (b.rate + b.outRate) - (a.rate + a.outRate) || b.lag - a.lag).slice(0, PART_MAX));
    hidden = rows.length - PART_MAX;
    rows = rows.filter((r) => keep.has(r));
  }
  const lines = [];
  const roles = [...new Set(rows.map((r) => r.role))];
  const topics = new Set(rows.map((r) => r.role + "/" + r.topic));
  const multiTopic = n.kind !== "topic" && topics.size > 1;
  let lastRole = null, lastTopic = null;
  for (const r of rows) {
    if (r.role !== lastRole) {
      lastRole = r.role;
      lastTopic = null;
      lines.push({ type: "head", role: r.role, withRole: roles.length > 1 });
    }
    if ((multiTopic || (roles.length > 1 && n.kind === "group")) && r.topic !== lastTopic) {
      lastTopic = r.topic;
      lines.push({ type: "topic", text: r.topic });
    }
    lines.push({ type: "row", row: r });
  }
  if (hidden) lines.push({ type: "note", text: `${PART_MAX} busiest of ${parts.length} partitions shown` });
  return lines;
}

function heightOf(n) {
  const lines = partitionLines(n);
  return lines ? NODE_H + 6 + lines.length * PART_ROW + 8 : NODE_H;
}

function partitionSvg(n, lines) {
  const rows = lines.filter((l) => l.type === "row").map((l) => l.row);
  const max = Math.max(1e-9, ...rows.map((r) => r.rate));
  const shortName = ownerNames(rows.map((r) => r.member).filter(Boolean));
  const x0 = 12, x1 = nodeW - 12, barW = x1 - x0;
  let y = NODE_H + 4;
  let out = `<line class="part-rule" x1="${x0}" y1="${NODE_H}" x2="${x1}" y2="${NODE_H}"/>`;
  for (const l of lines) {
    const base = y + PART_ROW - 5;
    if (l.type === "note") {
      out += `<text class="part-note" x="${x0 + 2}" y="${base}">${esc(l.text)}</text>`;
    } else if (l.type === "topic") {
      out += `<text class="part-topic" x="${x0 + 2}" y="${base}">${esc(clip(l.text, fit(30)))}</text>`;
    } else if (l.type === "head") {
      const title = { topic: "Partition", read: l.withRole ? "Reads" : "Partition", write: l.withRole ? "Writes" : "Partition" }[l.role];
      out += `<text class="part-head" x="${x0 + 2}" y="${base}">${title}</text>`;
      if (l.role === "topic") {
        out += `<text class="part-head" x="${x1 - 104}" y="${base}" text-anchor="end">in/s</text>`
          + `<text class="part-head" x="${x1 - 52}" y="${base}" text-anchor="end">out/s</text>`
          + `<text class="part-head" x="${x1 - 2}" y="${base}" text-anchor="end">lag</text>`;
      } else if (l.role === "read") {
        out += `<text class="part-head" x="${x0 + 30}" y="${base}">owner</text>`
          + `<text class="part-head" x="${x1 - 52}" y="${base}" text-anchor="end">msg/s</text>`
          + `<text class="part-head" x="${x1 - 2}" y="${base}" text-anchor="end">lag</text>`;
      } else {
        out += `<text class="part-head" x="${x1 - 2}" y="${base}" text-anchor="end">msg/s</text>`;
      }
    } else {
      const r = l.row;
      const key = `${n.id}#${r.role}#${r.topic}#${r.partition}`;
      const rising = r.role !== "write" && lagRising(key);
      const w = (r.rate / max) * barW;                 // bar = write rate (topic, producer) or read rate (group)
      const tip = r.role === "topic"
        ? `${r.topic} partition ${r.partition}: ${fmtRate(r.rate)} in, ${fmtRate(r.outRate)} out, lag ${fmtNum(r.lag)}, end offset ${fmtNum(r.offset)}`
        : r.role === "read"
          ? `${r.topic} partition ${r.partition}: ${fmtRate(r.rate)}, lag ${fmtNum(r.lag)}${r.member ? `, owned by ${r.member}` : ", unassigned"}`
          : `${r.topic} partition ${r.partition}: ${fmtRate(r.rate)} written`;
      out += `<g class="part${r.rate === 0 && r.outRate === 0 ? " zero" : ""}"><title>${esc(tip)}</title>`
        + `<rect class="part-bar ${r.role}" x="${x0}" y="${y + 2}" width="${Math.max(w, 0).toFixed(1)}" height="${PART_ROW - 3}" rx="2"/>`
        + `<text class="part-p" x="${x0 + 4}" y="${base}">${r.partition}</text>`;
      if (r.role === "topic") {
        out += `<text class="part-v in" x="${x1 - 104}" y="${base}" text-anchor="end">${fmtNum(r.rate)}</text>`
          + `<text class="part-v out" x="${x1 - 52}" y="${base}" text-anchor="end">${fmtNum(r.outRate)}</text>`
          + `<text class="part-v${rising ? " lag" : ""}" x="${x1 - 2}" y="${base}" text-anchor="end">${fmtNum(r.lag)}${rising ? " ↑" : ""}</text>`;
      } else if (r.role === "read") {
        const short = r.member ? shortName(r.member) : "unassigned";
        const owner = short.length > 13 ? "…" + short.slice(-12) : short;   // the end is what tells members apart
        out += `<text class="part-m" x="${x0 + 30}" y="${base}">${esc(owner)}</text>`
          + `<text class="part-v out" x="${x1 - 52}" y="${base}" text-anchor="end">${fmtNum(r.rate)}</text>`
          + `<text class="part-v${rising ? " lag" : ""}" x="${x1 - 2}" y="${base}" text-anchor="end">${fmtNum(r.lag)}${rising ? " ↑" : ""}</text>`;
      } else {
        out += `<text class="part-v in" x="${x1 - 2}" y="${base}" text-anchor="end">${fmtNum(r.rate)}</text>`;
      }
      out += "</g>";
    }
    y += PART_ROW;
  }
  return out;
}

/**
 * Member client ids usually share a long prefix ("consumer-inventory-sync-1", "...-2"). Drop the shared part,
 * keeping the last word before the difference, so rows read "sync-1", "sync-2". Full names are in tooltips.
 */
function ownerNames(names) {
  const uniq = [...new Set(names)];
  if (uniq.length < 2) return (n) => n;
  let prefix = uniq[0];
  for (const n of uniq) while (!n.startsWith(prefix)) prefix = prefix.slice(0, -1);
  const seps = [...prefix.matchAll(/[-_.]/g)].map((m) => m.index);
  const cut = seps.length >= 2 ? seps[seps.length - 2] + 1 : 0;
  return (n) => n.slice(cut);
}

async function fetchPartitions(id) {
  try {
    const res = await fetch("api/partitions?node=" + encodeURIComponent(id), { cache: "no-store" });
    if (!res.ok) return;
    const data = await res.json();
    if (state.selected !== id) return;               // selection changed while fetching
    const t = data.timestamp;
    for (const r of data.partitions) remember(`${id}#${r.role}#${r.topic}#${r.partition}`, r.rate, r.lag, t);
    state.parts = { node: id, rows: data.partitions };
    render();
  } catch (e) { /* next snapshot retries */ }
}

function place(nodes, anchorY) {
  const items = nodes.map((n) => {
    const ys = anchorY(n);
    return { n, want: ys.length ? ys.reduce((a, b) => a + b, 0) / ys.length : Infinity };
  });
  items.sort((a, b) => a.want - b.want || nameOf(a.n).localeCompare(nameOf(b.n)));
  let next = 0;
  for (const it of items) {
    const want = isFinite(it.want) ? it.want : next;
    it.n._y = Math.max(want, next);
    next = it.n._y + heightOf(it.n) + (ROW - NODE_H);
  }
  // Pull the column back up if it was pushed far below its anchors.
  if (items.length) {
    const overflow = items.reduce((m, it) => Math.max(m, it.n._y - (isFinite(it.want) ? it.want : it.n._y)), 0);
    const shift = Math.min(overflow / 2, items[0].n._y);
    for (const it of items) it.n._y -= shift;
  }
}

const nameOf = (n) => n.name || n.groupId || n.label || "";

function layout(g) {
  const { cols, succ, pred } = columnsOf(g);
  const width = els.canvas.clientWidth - 56;
  const k = Math.max(cols.length, 1);
  // Deep chains get tighter gaps first; past that the canvas scrolls sideways rather than squeezing the text.
  const gap = k > 1 ? Math.max(90, Math.min(380, (width - nodeW * k) / (k - 1))) : 0;

  const all = cols.flat();
  for (const n of all) n._y = undefined;
  const byId = new Map(all.map((n) => [n.id, n]));
  const ys = (list) => list.map((id) => byId.get(id)?._y).filter((y) => y != null);

  // Anchor on the first column of topics, alphabetically, then spread outwards following the links.
  let anchor = cols.findIndex((c) => c.some((n) => n.kind === "topic"));
  if (anchor < 0) anchor = 0;
  cols[anchor].sort((x, y) => (x.kind === "topic" ? 0 : 1) - (y.kind === "topic" ? 0 : 1) || nameOf(x).localeCompare(nameOf(y)));
  let yAcc = 0;
  for (const n of cols[anchor]) { n._y = yAcc; yAcc += heightOf(n) + (ROW - NODE_H); }
  for (let pass = 0; pass < 2; pass++) {
    for (let c = anchor + 1; c < cols.length; c++) {
      place(cols[c], (n) => { const p = ys(pred.get(n.id)); return p.length ? p : ys(succ.get(n.id)); });
    }
    for (let c = anchor - 1; c >= 0; c--) {
      place(cols[c], (n) => { const s = ys(succ.get(n.id)); return s.length ? s : ys(pred.get(n.id)); });
    }
  }

  const columns = cols.map((nodes, i) => ({ x: i * (nodeW + gap), nodes }));
  const height = all.reduce((m, n) => Math.max(m, n._y + heightOf(n)), 0) + 12;
  return { columns, width: columns.length ? columns[columns.length - 1].x + nodeW : 0, height };
}

function columnTitle(nodes) {
  const kinds = new Set(nodes.map((n) => n.kind));
  const names = [];
  if (kinds.has("producer")) names.push("producers");
  if (kinds.has("topic")) names.push("topics");
  if (kinds.has("group")) names.push("consumer groups");
  const t = names.join(" and ");
  return t.charAt(0).toUpperCase() + t.slice(1);
}

// ---------- rendering ----------

function linkWidth(rate) {
  return rate > 0 ? Math.min(11, 1.6 + 1.7 * Math.log10(1 + rate)) : 1.2;
}

function render() {
  const s = state.snap;
  const g = visibleGraph();
  const { columns, width, height } = layout(g);
  const svg = els.svg;

  els.heads.style.width = width + "px";
  els.heads.innerHTML = columns.map((c) => `<h2 style="left:${c.x}px;max-width:${nodeW}px">${esc(columnTitle(c.nodes))}</h2>`).join("");

  if (!g.topics.length && !g.groups.length) {
    svg.setAttribute("width", 0);
    svg.setAttribute("height", 0);
    svg.innerHTML = "";
    els.heads.innerHTML = "";
    state.animated = [];
    els.empty.hidden = false;
    els.empty.innerHTML = els.filter.value || els.hideIdle.checked
      ? `<strong>Nothing matches</strong>No active topic matches the current filter. Clear the filter or show idle topics.`
      : s.error && !s.topics.length
        ? `<strong>No data yet</strong>The cluster has not answered. Check the bootstrap servers and security settings in your config.`
        : `<strong>No topics yet</strong>Topics appear here as soon as they are created.`;
    return;
  }
  els.empty.hidden = true;
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

  const pos = new Map();
  for (const c of columns) for (const n of c.nodes) pos.set(n.id, { x: c.x, y: n._y });

  // Links first so nodes sit on top.
  let linkSvg = "";
  for (const l of g.links) {
    const a = pos.get(l.source), b = pos.get(l.target);
    if (!a || !b) continue;
    const x1 = a.x + nodeW, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2;
    const bend = Math.max(70, Math.abs(x2 - x1) / 2);   // also draws loops when a link points back left
    const d = `M${x1},${y1} C${x1 + bend},${y1} ${x2 - bend},${y2} ${x2},${y2}`;
    const w = linkWidth(l.rate);
    const lagging = l.kind === "consume" && lagRising(l.id);
    const cls = `${l.kind}${lagging ? " lagging" : ""}`;
    const tip = l.kind === "produce"
      ? `${labelOf(l.source)} → ${labelOf(l.target)}: ${fmtRate(l.rate)}`
      : `${labelOf(l.source)} → ${labelOf(l.target)}: ${fmtRate(l.rate)}, lag ${fmtNum(l.lag)}${lagging ? " (rising)" : ""}`;
    if (l.rate > 0) {
      const gapLen = Math.max(9, 34 - 6 * Math.log10(1 + l.rate));
      linkSvg += `<g data-ends="${esc(l.source)} ${esc(l.target)}"><title>${esc(tip)}</title>`
        + `<path class="link-base ${cls}" d="${d}" stroke-width="${(w + 3).toFixed(1)}"/>`
        + `<path class="link ${cls}" data-id="${esc(l.id)}" data-rate="${l.rate}" d="${d}" stroke-width="${w.toFixed(1)}" stroke-dasharray="0.1 ${gapLen.toFixed(1)}"/></g>`;
    } else {
      linkSvg += `<g data-ends="${esc(l.source)} ${esc(l.target)}"><title>${esc(tip)}</title>`
        + `<path class="link idle${lagging ? " lagging" : ""}" d="${d}" stroke-width="1.4"/></g>`;
    }
  }

  let nodeSvg = "";
  for (const n of g.producers) nodeSvg += producerNode(n, pos.get(n.id));
  for (const n of g.topics) nodeSvg += topicNode(n, pos.get(n.id));
  for (const n of g.groups) nodeSvg += groupNode(n, pos.get(n.id));

  svg.innerHTML = `<g class="links">${linkSvg}</g><g class="nodes">${nodeSvg}</g>`;

  state.animated = [...svg.querySelectorAll("path.link[data-rate]")].map((el) => ({
    el, id: el.dataset.id, speed: 22 + 26 * Math.log10(1 + Number(el.dataset.rate)),
    period: el.getAttribute("stroke-dasharray").split(" ").reduce((a, b) => a + Number(b), 0),
  }));
  // Start where the previous render left off so particles don't jump every poll.
  for (const a of state.animated) a.el.style.strokeDashoffset = state.offsets.get(a.id) || 0;
  applyFocus();
}

function labelOf(id) {
  const n = state.nodes.get(id);
  return n ? n.label || n.name || n.groupId : id;
}

function nodeShell(n, p, cls, title, body) {
  const lines = partitionLines(n);
  const selected = lines ? " selected expanded" : "";
  return `<g class="node ${cls}${selected}" data-id="${esc(n.id)}" transform="translate(${p.x},${p.y})" tabindex="0" role="button" aria-expanded="${lines ? "true" : "false"}" aria-label="${esc(title)}">`
    + `<title>${esc(title)}</title><rect class="box" width="${nodeW}" height="${heightOf(n)}" rx="7"/>${body}`
    + (lines ? partitionSvg(n, lines) : "") + `</g>`;
}

function producerNode(n, p) {
  const idle = n.rate === 0;
  const sub = n.synthetic
    ? `<tspan class="rate-in">${fmtRate(n.rate)}</tspan> not attributed to a producer`
    : `<tspan class="rate-in">${fmtRate(n.rate)}</tspan>${n.topics.length > 1 ? ` into ${n.topics.length} topics` : ""}${idle ? `, last write ${ago(n.lastSeenMs)}` : ""}`;
  return nodeShell(n, p, `producer${n.synthetic ? " synthetic" : ""}${idle ? " idle" : ""}`,
    `${n.label}: ${fmtRate(n.rate)}`,
    `<line class="accent" x1="${nodeW - 1.5}" y1="12" x2="${nodeW - 1.5}" y2="${NODE_H - 12}"/>`
    + `<text class="name" x="14" y="21">${esc(clip(n.label, fit(28)))}</text>`
    + `<text class="sub" x="14" y="39">${sub}</text>`);
}

function topicNode(n, p) {
  const idle = n.inRate === 0 && n.outRate === 0;
  return nodeShell(n, p, `topic${idle ? " idle" : ""}`,
    `${n.name}: ${fmtRate(n.inRate)} in, ${fmtRate(n.outRate)} out, ${n.partitions} partitions`,
    `<text class="name" x="14" y="21">${esc(clip(n.name, fit(64)))}</text>`
    + `<text class="sub" x="${nodeW - 12}" y="21" text-anchor="end">${n.partitions}p</text>`
    + `<text class="sub" x="14" y="39"><tspan class="rate-in">${fmtRate(n.inRate)}</tspan> in   `
    + `<tspan class="rate-out">${fmtRate(n.outRate)}</tspan> out</text>`
    + `<text class="sub" x="${nodeW - 12}" y="39" text-anchor="end">${fmtNum(n.messages)} msgs</text>`);
}

function groupNode(n, p) {
  const rising = lagRising(n.id);
  const relay = n.outputTopics.length > 0;
  const idle = n.rate === 0 && !(n.lag > 0) && !(n.produceRate > 0);
  const lagText = n.lag > 0 ? `lag ${fmtNum(n.lag)}${rising ? " ↑" : ""}` : "no lag";
  const members = n.members.length;
  const memberText = members ? members + (members === 1 ? " member" : " members") : n.state.toLowerCase();
  // A processor shows what it reads and writes; its lag moves to the top right.
  const corner = relay ? `<tspan class="${rising ? "lag" : ""}">${lagText}</tspan>` : memberText;
  const sub = relay
    ? `<tspan class="rate-out">${fmtRate(n.rate)}</tspan> in   <tspan class="rate-in">${fmtRate(n.produceRate)}</tspan> out`
    : `<tspan class="rate-out">${fmtRate(n.rate)}</tspan>   <tspan class="${rising ? "lag" : ""}">${lagText}</tspan>`;
  return nodeShell(n, p, `group${relay ? " relay" : ""}${idle ? " idle" : ""}`,
    `${n.groupId}: ${fmtRate(n.rate)} consumed${relay ? `, ${fmtRate(n.produceRate)} produced` : ""}, ${lagText}, ${memberText}, ${n.state}`,
    `<line class="accent" x1="1.5" y1="12" x2="1.5" y2="${NODE_H - 12}"/>`
    + (relay ? `<line class="accent out" x1="${nodeW - 1.5}" y1="12" x2="${nodeW - 1.5}" y2="${NODE_H - 12}"/>` : "")
    + `<text class="name" x="14" y="21">${esc(clip(n.groupId, fit(relay ? 96 : 100)))}</text>`
    + `<text class="sub" x="${nodeW - 12}" y="21" text-anchor="end">${corner}</text>`
    + `<text class="sub" x="14" y="39">${sub}</text>`);
}

// ---------- hover / selection highlighting ----------

/** Highlights the whole lineage of the focused node: everything upstream and downstream of it. */
function applyFocus() {
  const focus = state.hovered || state.selected;
  const svg = els.svg;
  svg.classList.toggle("focusing", !!focus);
  svg.querySelectorAll(".hl").forEach((e) => e.classList.remove("hl"));
  if (!focus) return;
  const links = state.snap.links;
  const related = new Set([focus]);
  const hlLinks = new Set();
  for (const forward of [true, false]) {
    const queue = [focus];
    const seen = new Set(queue);
    while (queue.length) {
      const id = queue.shift();
      for (const l of links) {
        const from = forward ? l.source : l.target, to = forward ? l.target : l.source;
        if (from !== id) continue;
        hlLinks.add(l.source + " " + l.target);
        related.add(to);
        if (!seen.has(to)) { seen.add(to); queue.push(to); }
      }
    }
  }
  svg.querySelectorAll(".node").forEach((e) => { if (related.has(e.dataset.id)) e.classList.add("hl"); });
  svg.querySelectorAll("g[data-ends]").forEach((e) => {
    if (hlLinks.has(e.dataset.ends)) e.querySelectorAll("path").forEach((p) => p.classList.add("hl"));
  });
}

els.svg.addEventListener("mouseover", (e) => {
  const n = e.target.closest(".node");
  const id = n ? n.dataset.id : null;
  if (id !== state.hovered) { state.hovered = id; applyFocus(); }
});
els.svg.addEventListener("mouseleave", () => { state.hovered = null; applyFocus(); });
els.svg.addEventListener("click", (e) => {
  const n = e.target.closest(".node");
  select(n ? n.dataset.id : null);
});
els.svg.addEventListener("keydown", (e) => {
  const n = e.target.closest(".node");
  if (n && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); select(n.dataset.id); }
});
document.addEventListener("keydown", (e) => { if (e.key === "Escape" && state.selected) select(null); });

function select(id) {
  state.selected = id === state.selected ? null : id;
  state.parts = null;
  render();
  renderDetail();
  if (state.selected) {
    fetchPartitions(state.selected);
    const el = els.svg.querySelector(`.node[data-id="${CSS.escape(state.selected)}"]`);
    if (el && document.activeElement !== el && document.activeElement?.closest?.(".node")) el.focus({ preventScroll: true });
  }
}

// ---------- detail panel ----------

function sparkline(id, key, color) {
  const h = (state.history.get(id) || []).map((p) => p[key] ?? 0);
  if (h.length < 2) return `<p class="spark-cap">Collecting history…</p>`;
  const w = 296, ht = 70, max = Math.max(...h, 1e-9);
  const step = w / Math.max(h.length - 1, 30);   // fill the width once there is enough history
  const x0 = w - (h.length - 1) * step;
  const pts = h.map((v, i) => `${(x0 + i * step).toFixed(1)},${(ht - 4 - (v / max) * (ht - 10)).toFixed(1)}`);
  const area = `M${x0.toFixed(1)},${ht} L${pts.join(" L")} L${w},${ht} Z`;
  const span = Math.round((h.length - 1) * (state.snap.pollIntervalMs / 1000));
  return `<svg class="spark" viewBox="0 0 ${w} ${ht}" preserveAspectRatio="none" aria-hidden="true">`
    + `<path d="${area}" fill="${color}" opacity=".14"/>`
    + `<polyline points="${pts.join(" ")}" fill="none" stroke="${color}" stroke-width="1.8" vector-effect="non-scaling-stroke"/></svg>`
    + `<p class="spark-cap">${key === "lag" ? "Lag" : "Rate"} over the last ${span >= 90 ? Math.round(span / 60) + " min" : span + " s"}, peak ${key === "lag" ? fmtNum(max) : fmtRate(max)}</p>`;
}

function linkList(items, empty) {
  if (!items.length) return `<p class="note">${empty}</p>`;
  return "<ul>" + items.map((it) =>
    `<li><span><button data-select="${esc(it.id)}">${esc(it.label)}</button></span><span class="num">${it.value}</span></li>`).join("") + "</ul>";
}

function renderDetail() {
  const id = state.selected;
  const n = id && state.nodes.get(id);
  if (!n) { els.detail.hidden = true; return; }
  const links = state.snap.links;
  const css = getComputedStyle(document.documentElement);
  const cProduce = css.getPropertyValue("--produce").trim();
  const cConsume = css.getPropertyValue("--consume").trim();
  const cLag = css.getPropertyValue("--lag").trim();
  let html = "";

  if (n.kind === "topic") {
    const ins = links.filter((l) => l.target === id).sort((a, b) => b.rate - a.rate);
    const outs = links.filter((l) => l.source === id).sort((a, b) => b.rate - a.rate);
    html = head("Topic", n.name)
      + stats([["In", fmtRate(n.inRate)], ["Out", fmtRate(n.outRate)], ["Consumer lag", fmtNum(n.lag)], ["Partitions", `${n.partitions} (RF ${n.replicationFactor})`], ["Messages written", fmtNum(n.messages)]])
      + sparkline(id, "rate", cProduce)
      + "<h4>Producers</h4>" + linkList(ins.map((l) => ({ id: l.source, label: labelOf(l.source), value: fmtRate(l.rate) })),
        "No producer wrote here recently.")
      + "<h4>Consumer groups</h4>" + linkList(outs.map((l) => ({ id: l.target, label: labelOf(l.target), value: `${fmtRate(l.rate)}, lag ${fmtNum(l.lag)}` })),
        "No consumer group reads this topic.");
  } else if (n.kind === "group") {
    const ins = links.filter((l) => l.target === id).sort((a, b) => b.rate - a.rate);
    const rising = lagRising(id);
    const outs = links.filter((l) => l.source === id).sort((a, b) => b.rate - a.rate);
    const relay = n.outputTopics.length > 0;
    html = head(relay ? "Consumer group, writes downstream" : "Consumer group", n.groupId, `<span class="pill${n.state === "STABLE" ? "" : " bad"}">${esc(n.state.toLowerCase())}</span>`)
      + stats([["Consuming", fmtRate(n.rate)], ...(relay ? [["Producing", fmtRate(n.produceRate)]] : []),
        ["Lag", fmtNum(n.lag) + (rising ? " ↑" : "")], ["Members", n.members.length]])
      + sparkline(id, "rate", cConsume)
      + sparkline(id, "lag", cLag)
      + "<h4>Reads from</h4>" + linkList(ins.map((l) => ({ id: l.source, label: labelOf(l.source), value: `${fmtRate(l.rate)}, lag ${fmtNum(l.lag)}` })),
        "This group has no committed offsets on the visible topics.")
      + (relay ? "<h4>Writes to</h4>" + linkList(outs.map((l) => ({ id: l.target, label: labelOf(l.target), value: fmtRate(l.rate) })), "No recent writes.")
        + `<p class="note">Linked through producer ${n.producerIds.length > 1 ? "ids" : "id"} ${esc(n.producerIds.join(", "))}, whose transactional id starts with this group id.</p>` : "")
      + "<h4>Members</h4>" + (n.members.length
        ? "<ul>" + n.members.map((m) => `<li><span>${esc(m.clientId)}<br><small class="num">${esc(m.host)}</small></span><span class="num">${m.partitions} part.</span></li>`).join("") + "</ul>"
        : `<p class="note">No active members. Committed offsets are kept, so lag still shows what is waiting.</p>`);
  } else {
    const outs = links.filter((l) => l.source === id).sort((a, b) => b.rate - a.rate);
    const rows = n.synthetic ? [["Writing", fmtRate(n.rate)], ["Topics", n.topics.length]]
      : [["Writing", fmtRate(n.rate)], ["Last write", ago(n.lastSeenMs)], ["Producer id", n.producerId], ["Transactional id", n.transactionalId || "none"]];
    html = head(n.synthetic ? "Unattributed traffic" : "Producer", n.label)
      + stats(rows)
      + sparkline(id, "rate", cProduce)
      + "<h4>Writes to</h4>" + linkList(outs.map((l) => ({ id: l.target, label: labelOf(l.target), value: fmtRate(l.rate) })), "No recent writes.")
      + (n.synthetic
        ? `<p class="note">Messages that no identified producer accounts for. Kafka only reports idempotent and transactional producers (the default since Kafka 3.0 clients); older or non-idempotent clients land here.</p>`
        : `<p class="note">Kafka identifies producers by id, not by client.id. Set a transactional.id to see a readable name.</p>`);
  }
  els.detail.innerHTML = html;
  els.detail.hidden = false;
}

function head(kind, title, extra = "") {
  return `<header><div><p class="kind">${esc(kind)} ${extra}</p><h3>${esc(title)}</h3></div>`
    + `<button class="close" data-close aria-label="Close details">×</button></header>`;
}

function stats(rows) {
  return `<dl class="stats">${rows.map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join("")}</dl>`;
}

els.detail.addEventListener("click", (e) => {
  if (e.target.closest("[data-close]")) { select(null); return; }
  const b = e.target.closest("[data-select]");
  if (b) select(b.dataset.select);
});

// ---------- flow animation ----------

let lastFrame = performance.now();
function frame(now) {
  const dt = Math.min(0.1, (now - lastFrame) / 1000);
  lastFrame = now;
  if (!reduceMotion && !document.hidden) {
    for (const a of state.animated) {
      const off = ((state.offsets.get(a.id) || 0) - a.speed * dt) % a.period;
      state.offsets.set(a.id, off);
      a.el.style.strokeDashoffset = off;
    }
  }
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);

// ---------- controls ----------

function savePrefs() {
  try { localStorage.setItem("kfv.prefs", JSON.stringify({ hideIdle: els.hideIdle.checked, filter: els.filter.value })); } catch (e) { /* storage unavailable */ }
}
try {
  const p = JSON.parse(localStorage.getItem("kfv.prefs") || "{}");
  els.hideIdle.checked = !!p.hideIdle;
  els.filter.value = p.filter || "";
} catch (e) { /* storage unavailable */ }

els.filter.addEventListener("input", () => { savePrefs(); if (state.snap) render(); });
els.hideIdle.addEventListener("change", () => { savePrefs(); if (state.snap) render(); });
let resizeTimer;
window.addEventListener("resize", () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(() => state.snap && render(), 120); });

// ---------- live connection ----------

function setLive(kind, text) {
  els.live.className = "live " + kind;
  els.liveText.textContent = text;
}

function connect() {
  const es = new EventSource("api/stream");
  es.addEventListener("snapshot", (e) => {
    const snap = JSON.parse(e.data);
    ingest(snap);
    setLive(snap.error ? "off" : "on", snap.error ? "Cluster unreachable" : "Live");
  });
  es.onerror = () => setLive("off", "Reconnecting to viewer");
}
connect();

setInterval(() => {
  const s = state.snap;
  if (s && Date.now() - state.lastMessageAt > Math.max(10000, s.pollIntervalMs * 4)) setLive("off", "No update for " + ago(state.lastMessageAt).replace(" ago", ""));
}, 2000);
