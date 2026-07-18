"use strict";
const path = require("path");
const fs = require("fs");
const http = require("http");
const express = require("express");
const { Server } = require("socket.io");

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: "*" } });

// ── HOST-BASED ROUTING ────────────────────────────────────────────────────────
// CNAME records can only point to a hostname, not a path, so we do path routing
// here in express based on the incoming Host header.
//
//   watch.skybound.at     → CNAME → summerday.onrender.com  → serves /client
//   broadcast.skybound.at → CNAME → summerday.onrender.com  → serves /broadcaster
//   admin.skybound.at     → CNAME → summerday.onrender.com  → serves /admin
//
// The user sets three CNAME records all pointing to summerday.onrender.com,
// and this middleware does the rest.

function hostRouter(req, res, next) {
  const host = (req.hostname || "").toLowerCase();
  if (host === "watch.skybound.at") {
    // Rewrite request path so static middleware below serves /client/*
    req.url = "/client" + (req.url === "/" ? "/" : req.url);
  } else if (host === "broadcast.skybound.at") {
    req.url = "/broadcaster" + (req.url === "/" ? "/" : req.url);
  } else if (host === "admin.skybound.at") {
    req.url = "/admin" + (req.url === "/" ? "/" : req.url);
  }
  next();
}

app.use(hostRouter);

// ── STATIC FILES ─────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, "public")));
app.use("/broadcaster", express.static(path.join(__dirname, "public", "broadcaster")));
app.use("/client",      express.static(path.join(__dirname, "public", "client")));
app.use("/admin",       express.static(path.join(__dirname, "public", "admin")));

// Explicit index routes (catches bare /broadcaster etc. without trailing slash)
app.get("/broadcaster", (req, res) => res.sendFile(path.join(__dirname, "public", "broadcaster", "index.html")));
app.get("/client",      (req, res) => res.sendFile(path.join(__dirname, "public", "client",      "index.html")));
app.get("/admin",       (req, res) => res.sendFile(path.join(__dirname, "public", "admin",       "index.html")));

// ── STATE ─────────────────────────────────────────────────────────────────────
const channelBroadcasters = new Map();   // channelId → broadcasterSocketId
const broadcasterChannels = new Map();   // broadcasterSocketId → channelId
const viewerChannels      = new Map();   // viewerSocketId → channelId
const adminSockets        = new Set();   // authenticated admin socket ids
const channelThumbnails   = new Map();   // channelId → latest snapshot dataURL (Station Content Preview)
const channelSchedules    = new Map();   // channelId → { items:[{name,description,duration}], currentIndex }
const stationCatalog      = new Map();   // channelId → { channelId, status, offlineScreen, config, isVerified, allowRebroadcast, rebroadcastStations }
const pendingApprovals    = new Map();   // requestId → { socketId, channelId, config, offlineScreen, requestedAt }
const STATIONS_FILE       = path.join(__dirname, "stations.json");

const ADMIN_PASSWORD = "skybound2025";

function defaultOfflineScreen(channelId) {
  const label = String(channelId || "Station").replace(/[<>&"]/g, "");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="720" viewBox="0 0 1280 720">
    <rect width="1280" height="720" fill="#15140F"/>
    <rect x="72" y="72" width="1136" height="576" fill="#1C1B15" stroke="#EFE6CF" stroke-width="6"/>
    <circle cx="640" cy="292" r="70" fill="#D6472C"/>
    <path d="M570 292h140" stroke="#F0AE1E" stroke-width="18" stroke-linecap="square"/>
    <text x="640" y="420" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="42" font-weight="700" fill="#F2ECDD" letter-spacing="8">OFFLINE</text>
    <text x="640" y="474" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="24" fill="#B8B09B">${label}</text>
  </svg>`;
}

function defaultOfflineDataUrl(channelId) {
  return "data:image/svg+xml;charset=utf-8," + encodeURIComponent(defaultOfflineScreen(channelId));
}

function loadStationCatalog() {
  try {
    const saved = JSON.parse(fs.readFileSync(STATIONS_FILE, "utf8"));
    if (!Array.isArray(saved)) return;
    saved.forEach(station => {
      if (!station || !station.channelId) return;
      stationCatalog.set(String(station.channelId), {
        channelId: String(station.channelId),
        status: "offline",
        offlineScreen: station.offlineScreen || defaultOfflineDataUrl(station.channelId),
        config: station.config && typeof station.config === "object" ? station.config : {},
        isVerified: station.isVerified !== false,
        allowRebroadcast: !!station.allowRebroadcast,
        rebroadcastStations: Array.isArray(station.rebroadcastStations) ? station.rebroadcastStations.slice(0, 12) : [],
      });
    });
  } catch (err) {
    if (err.code !== "ENOENT") console.warn("Could not load station catalog:", err.message);
  }
}

function saveStationCatalog() {
  try {
    const records = Array.from(stationCatalog.values()).map(station => ({
      channelId: station.channelId,
      offlineScreen: station.offlineScreen || "",
      config: station.config || {},
      isVerified: station.isVerified !== false,
      allowRebroadcast: !!station.allowRebroadcast,
      rebroadcastStations: Array.isArray(station.rebroadcastStations) ? station.rebroadcastStations.slice(0, 12) : [],
    }));
    const tempFile = STATIONS_FILE + ".tmp";
    fs.writeFileSync(tempFile, JSON.stringify(records, null, 2), "utf8");
    fs.renameSync(tempFile, STATIONS_FILE);
  } catch (err) {
    console.warn("Could not save station catalog:", err.message);
  }
}

function stationRecord(channelId) {
  const existing = stationCatalog.get(channelId) || {};
  return {
    channelId,
    status: channelBroadcasters.has(channelId) ? "live" : "offline",
    offlineScreen: existing.offlineScreen || defaultOfflineDataUrl(channelId),
    config: existing.config || {},
    isVerified: true,
    allowRebroadcast: !!existing.allowRebroadcast,
    rebroadcastStations: Array.isArray(existing.rebroadcastStations) ? existing.rebroadcastStations : [],
  };
}

loadStationCatalog();

function listChannels() {
  const ids = new Set([...stationCatalog.keys(), ...channelBroadcasters.keys()]);
  return Array.from(ids).map(id => stationRecord(id));
}

function listPendingApprovals() {
  return Array.from(pendingApprovals.entries()).map(([requestId, req]) => ({
    requestId,
    channelId: req.channelId,
    requestedAt: req.requestedAt,
  }));
}

function notifyAdmins(event, payload) {
  adminSockets.forEach(id => io.to(id).emit(event, payload));
}

function publishChannels() {
  const channels = listChannels();
  io.emit("channels-updated", channels);
  notifyAdmins("admin:channels-updated", channels);
}

function markStationOffline(channelId) {
  if (!channelId) return;
  const record = stationRecord(channelId);
  record.status = "offline";
  stationCatalog.set(channelId, record);
  channelThumbnails.delete(channelId);
  channelSchedules.delete(channelId);
  publishChannels();
  io.to(`viewers:${channelId}`).emit("broadcaster-left", {
    channelId,
    offlineScreen: record.offlineScreen,
  });
}

// ── SOCKET.IO ─────────────────────────────────────────────────────────────────
io.on("connection", (socket) => {
  socket.emit("channels-updated", listChannels());
  // Send whatever Station Content Preview snapshots we already have so a
  // freshly-connected client sees content for stations that were already
  // live before this client showed up — not just an empty placeholder.
  channelThumbnails.forEach((dataUrl, id) => {
    if (channelBroadcasters.has(id)) socket.emit("channel-thumbnail", { channelId: id, dataUrl });
  });
  // Same idea for program schedules (Now Playing / Up Next / full guide).
  channelSchedules.forEach((schedule, id) => {
    if (channelBroadcasters.has(id)) socket.emit("channel-schedule", { channelId: id, schedule });
  });

  // ── BROADCASTER ─────────────────────────────────────────────────────────────
  socket.on("broadcaster-register", (payload, ack) => {
    const data = typeof payload === "object" && payload ? payload : { channelId: payload };
    const id = String(data.channelId || "").trim().slice(0, 64) || "canal-1";
    const savedStation = stationCatalog.get(id);
    if (!data.isVerified && !savedStation?.isVerified) {
      const requestId = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
      const req = {
        socketId: socket.id,
        channelId: id,
        config: data.config || {},
        offlineScreen: data.offlineScreen || "",
        requestedAt: Date.now(),
      };
      pendingApprovals.set(requestId, req);
      notifyAdmins("admin:go-live-request", { requestId, channelId: id, requestedAt: req.requestedAt });
      if (typeof ack === "function") ack({ ok: false, pendingApproval: true, message: "Waiting for admin approval." });
      return;
    }
    if (channelBroadcasters.has(id) && channelBroadcasters.get(id) !== socket.id) {
      if (typeof ack === "function") ack({ ok: false, error: "Canal já está no ar." });
      return;
    }
    stationCatalog.set(id, {
      channelId: id,
      status: "live",
      offlineScreen: data.offlineScreen || stationCatalog.get(id)?.offlineScreen || defaultOfflineDataUrl(id),
      config: data.config || stationCatalog.get(id)?.config || {},
      isVerified: true,
      allowRebroadcast: !!data.allowRebroadcast,
      rebroadcastStations: Array.isArray(data.rebroadcastStations) ? data.rebroadcastStations.slice(0, 12) : [],
    });
    saveStationCatalog();
    channelBroadcasters.set(id, socket.id);
    broadcasterChannels.set(socket.id, id);
    socket.join(`broadcaster:${id}`);
    publishChannels();
    if (typeof ack === "function") ack({ ok: true, channelId: id });
  });

  socket.on("broadcaster-unregister", () => {
    const ch = broadcasterChannels.get(socket.id);
    if (ch) {
      channelBroadcasters.delete(ch);
      broadcasterChannels.delete(socket.id);
      markStationOffline(ch);
    }
  });

  // Station Content Preview: broadcaster periodically pushes a small
  // snapshot of its output canvas; we relay it to everyone so the client's
  // preview grid shows real content instead of a static placeholder.
  socket.on("broadcaster-thumbnail", ({ channelId, dataUrl } = {}) => {
    const ch = broadcasterChannels.get(socket.id);
    if (!ch || ch !== channelId || !dataUrl) return; // only the registered owner may update it
    channelThumbnails.set(ch, dataUrl);
    io.emit("channel-thumbnail", { channelId: ch, dataUrl });
  });

  // Program schedule: broadcaster pushes its ordered list of programs
  // (name/description/duration) plus which one is currently playing, so
  // viewers can see "Now Playing", "Up Next", and a full program guide.
  socket.on("broadcaster-schedule", ({ channelId, schedule } = {}) => {
    const ch = broadcasterChannels.get(socket.id);
    if (!ch || ch !== channelId || !schedule || !Array.isArray(schedule.items)) return;
    channelSchedules.set(ch, schedule);
    io.emit("channel-schedule", { channelId: ch, schedule });
  });

  socket.on("broadcaster-config", ({ channelId, allowRebroadcast, rebroadcastStations } = {}) => {
    const ch = broadcasterChannels.get(socket.id);
    if (!ch || ch !== channelId) return;
    const record = stationRecord(ch);
    record.allowRebroadcast = !!allowRebroadcast;
    record.rebroadcastStations = Array.isArray(rebroadcastStations) ? rebroadcastStations.slice(0, 12) : [];
    stationCatalog.set(ch, record);
    saveStationCatalog();
    publishChannels();
  });

  // ── VIEWER ──────────────────────────────────────────────────────────────────
  socket.on("viewer-join", (channelId) => {
    const id = String(channelId || "").trim();
    const bcId = channelBroadcasters.get(id);
    if (!bcId) {
      const station = stationCatalog.get(id);
      if (station) socket.emit("station-offline", { channelId: id, offlineScreen: station.offlineScreen || defaultOfflineDataUrl(id) });
      else socket.emit("viewer-error", { message: "Canal indisponível" });
      return;
    }
    socket.join(`viewers:${id}`);
    viewerChannels.set(socket.id, id);
    io.to(bcId).emit("viewer-ready", { viewerId: socket.id, channelId: id });
  });

  socket.on("viewer-leave", (channelId) => {
    const id = String(channelId || "").trim();
    socket.leave(`viewers:${id}`);
    viewerChannels.delete(socket.id);
    const bcId = channelBroadcasters.get(id);
    if (bcId) io.to(bcId).emit("viewer-gone", { viewerId: socket.id });
  });

  socket.on("signal", ({ to, data }) => {
    if (!to || !data) return;
    io.to(to).emit("signal", { from: socket.id, data });
  });

  // ── ADMIN ───────────────────────────────────────────────────────────────────
  socket.on("admin:auth", (password, ack) => {
    if (password === ADMIN_PASSWORD) {
      adminSockets.add(socket.id);
      socket.join("admins");
      if (typeof ack === "function") ack({ ok: true, channels: listChannels(), pendingApprovals: listPendingApprovals() });
    } else {
      if (typeof ack === "function") ack({ ok: false, error: "Wrong password." });
    }
  });

  socket.on("admin:approve-station", (requestId, ack) => {
    if (!adminSockets.has(socket.id)) { if (typeof ack === "function") ack({ ok: false }); return; }
    const req = pendingApprovals.get(requestId);
    if (!req) { if (typeof ack === "function") ack({ ok: false, error: "Request not found." }); return; }
    pendingApprovals.delete(requestId);
    io.to(req.socketId).emit("admin:go-live-approved", {
      channelId: req.channelId,
      offlineScreen: req.offlineScreen || defaultOfflineDataUrl(req.channelId),
    });
    notifyAdmins("admin:pending-approvals", listPendingApprovals());
    if (typeof ack === "function") ack({ ok: true });
  });

  socket.on("admin:reject-station", (requestId, ack) => {
    if (!adminSockets.has(socket.id)) { if (typeof ack === "function") ack({ ok: false }); return; }
    const req = pendingApprovals.get(requestId);
    if (!req) { if (typeof ack === "function") ack({ ok: false, error: "Request not found." }); return; }
    pendingApprovals.delete(requestId);
    io.to(req.socketId).emit("admin:go-live-rejected", { channelId: req.channelId });
    notifyAdmins("admin:pending-approvals", listPendingApprovals());
    if (typeof ack === "function") ack({ ok: true });
  });

  // Admin pushes a command to all viewers of a channel (or all channels)
  // cmd: { type: "toast"|"image"|"video"|"audio"|"takedown", channelId: "all"|"<id>", ...payload }
  socket.on("admin:push", (cmd) => {
    if (!adminSockets.has(socket.id)) return; // silently reject unauthenticated
    const { channelId, ...payload } = cmd;
    if (channelId === "all") {
      io.emit("admin:command", payload);
    } else {
      io.to(`viewers:${channelId}`).emit("admin:command", payload);
      // Also send to broadcaster so they see it too
      const bcId = channelBroadcasters.get(channelId);
      if (bcId) io.to(bcId).emit("admin:command", payload);
    }
  });

  // Admin takes a channel off-air (kicks the broadcaster)
  socket.on("admin:takedown", (channelId, ack) => {
    if (!adminSockets.has(socket.id)) { if (typeof ack === "function") ack({ ok: false }); return; }
    const bcId = channelBroadcasters.get(channelId);
    if (!bcId) { if (typeof ack === "function") ack({ ok: false, error: "Channel not found" }); return; }
    // Tell broadcaster they've been taken down
    io.to(bcId).emit("admin:forced-takedown", { reason: "Taken off-air by administrator." });
    channelBroadcasters.delete(channelId);
    broadcasterChannels.delete(bcId);
    channelThumbnails.delete(channelId);
    channelSchedules.delete(channelId);
    markStationOffline(channelId);
    if (typeof ack === "function") ack({ ok: true });
  });

  // ── DISCONNECT ──────────────────────────────────────────────────────────────
  socket.on("disconnect", () => {
    adminSockets.delete(socket.id);
    let removedPending = false;
    for (const [requestId, req] of pendingApprovals.entries()) {
      if (req.socketId === socket.id) {
        pendingApprovals.delete(requestId);
        removedPending = true;
      }
    }
    if (removedPending) notifyAdmins("admin:pending-approvals", listPendingApprovals());

    const ch = broadcasterChannels.get(socket.id);
    if (ch) {
      channelBroadcasters.delete(ch);
      broadcasterChannels.delete(socket.id);
      markStationOffline(ch);
      return;
    }
    const vc = viewerChannels.get(socket.id);
    if (vc) {
      viewerChannels.delete(socket.id);
      const bcId = channelBroadcasters.get(vc);
      if (bcId) io.to(bcId).emit("viewer-gone", { viewerId: socket.id });
    }
  });
});

// ── START ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    console.error(`\n❌ Port ${PORT} already in use.\n`);
    process.exit(1);
  } else throw err;
});

server.listen(PORT, () => {
  console.log(`\n✅  http://localhost:${PORT}`);
  console.log(`📡  Broadcaster: http://localhost:${PORT}/broadcaster/`);
  console.log(`📺  Client:      http://localhost:${PORT}/client/`);
  console.log(`🛡️   Admin:       http://localhost:${PORT}/admin/`);
  console.log(`\n  CNAME setup (all → summerday.onrender.com):`);
  console.log(`    watch.skybound.at     → /client`);
  console.log(`    broadcast.skybound.at → /broadcaster`);
  console.log(`    admin.skybound.at     → /admin\n`);
});
