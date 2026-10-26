#!/usr/bin/env node
/**
 * mesh-node — 常驻 mesh 节点（与 WorkBuddy 生命周期无关）
 *
 * 角色：
 *   1. 对 DSH 而言，它是一个合法的 mesh 对端：组播心跳 + /mesh/v1/* 五个端点
 *   2. 对本机 agent 而言，它是一条可靠的信箱：落库、长轮询、thread 匹配
 *
 * 设计要点：
 *   - 所有消息先落盘再返回，进程重启不丢
 *   - wait 是可中断的长轮询，不是 sleep 轮询
 *   - 端口独立于 DSH（默认 45930），靠组播报文里的 port 字段被对端发现
 */

import { createServer } from "node:http";
import { createSocket } from "node:dgram";
import { readFileSync, writeFileSync, appendFileSync, existsSync, mkdirSync } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import { hostname, platform, release, userInfo, networkInterfaces, homedir } from "node:os";
import { join } from "node:path";

const GROUP = "239.255.42.99";
const MULTICAST_PORT = 45892;
const APP_TAG = "dsh-mesh";
const PROTOCOL_VERSION = 2;

const PORT = Number(process.env.MESH_NODE_PORT || 45930);
const HOME = process.env.MESH_NODE_HOME || join(homedir(), ".mesh-node");
const STATE_DIR = HOME;
const INBOX_FILE = join(STATE_DIR, "inbox.jsonl");
const ID_FILE = join(STATE_DIR, "identity.json");
const CONFIG_FILE = join(STATE_DIR, "config.json");

mkdirSync(STATE_DIR, { recursive: true });

// ── 身份 ─────────────────────────────────────────────

function loadIdentity() {
  if (existsSync(ID_FILE)) {
    try {
      return JSON.parse(readFileSync(ID_FILE, "utf8"));
    } catch {}
  }
  const id = randomBytes(8).toString("hex");
  const shortHost = hostname().split(".")[0] || hostname();
  const rec = { id, machine: `${shortHost}-wb`, createdAt: new Date().toISOString() };
  writeFileSync(ID_FILE, JSON.stringify(rec, null, 2));
  return rec;
}

const identity = loadIdentity();
// 允许用环境变量覆盖机器名（同机多实例、或想要一个更好认的名字时）
if (process.env.MESH_NODE_MACHINE) identity.machine = process.env.MESH_NODE_MACHINE;

function loadConfig() {
  const dflt = { probeLocalDsh: true, localDshPort: 45917, seedPeers: [] };
  if (!existsSync(CONFIG_FILE)) return dflt;
  try {
    return { ...dflt, ...JSON.parse(readFileSync(CONFIG_FILE, "utf8")) };
  } catch {
    return dflt;
  }
}

const config = loadConfig();

// ── 本机地址 ─────────────────────────────────────────

function preferredAddress() {
  const ifs = networkInterfaces();
  const cands = [];
  for (const [name, addrs] of Object.entries(ifs)) {
    for (const a of addrs || []) {
      if (a.family !== "IPv4" || a.internal) continue;
      cands.push({ name, address: a.address });
    }
  }
  const lan = cands.find((c) => /^(192\.168|10\.|172\.(1[6-9]|2\d|3[01]))/.test(c.address));
  return (lan || cands[0] || { address: "127.0.0.1" }).address;
}

const SELF_HOST = preferredAddress();

// ── 收件箱（落盘）────────────────────────────────────

const inbox = []; // {id, from, fromMachine, task, thread, receivedAt, consumed, replyTo}
const seen = new Set();

function loadInbox() {
  if (!existsSync(INBOX_FILE)) return;
  const lines = readFileSync(INBOX_FILE, "utf8").split("\n").filter(Boolean);
  for (const line of lines) {
    try {
      const m = JSON.parse(line);
      if (m.id && !seen.has(m.id)) {
        seen.add(m.id);
        inbox.push(m);
      }
    } catch {}
  }
}

function persist(m) {
  appendFileSync(INBOX_FILE, JSON.stringify(m) + "\n");
}

function addInbound(fields) {
  const id = fields.id || `t-${randomBytes(6).toString("hex")}`;
  if (seen.has(id)) return { duplicate: true, id };
  seen.add(id);
  const msg = {
    id,
    from: String(fields.from || "unknown"),
    fromMachine: String(fields.fromMachine || ""),
    task: String(fields.task || ""),
    thread: String(fields.thread || ""),
    replyTo: String(fields.replyTo || ""),
    receivedAt: new Date().toISOString(),
    consumed: false,
  };
  inbox.push(msg);
  persist(msg);
  scheduleFlush().catch(() => {});
  // 唤醒等待者（只交给第一个匹配的，并标记已消费）
  for (const w of [...waiters]) {
    if (!w.thread || w.thread === msg.thread) {
      msg.consumed = true;
      w.resolve({ ok: true, message: msg });
      waiters.delete(w);
      break;
    }
  }
  return { duplicate: false, id };
}

let flushTimer = null;
function scheduleFlush() {
  if (flushTimer) return Promise.resolve();
  flushTimer = setTimeout(() => {
    flushTimer = null;
    process.stderr.write(`[mesh-node] inbox=${inbox.length}\n`);
  }, 50);
  return Promise.resolve();
}

// ── 长轮询等待者 ─────────────────────────────────────

const waiters = new Set();

function waitFor(thread, timeoutMs) {
  const hit = inbox.find((m) => !m.consumed && (!thread || m.thread === thread));
  if (hit) {
    hit.consumed = true;
    return Promise.resolve({ ok: true, message: hit });
  }
  return new Promise((resolve) => {
    const w = { thread, resolve };
    waiters.add(w);
    const timer = setTimeout(() => {
      waiters.delete(w);
      resolve({ ok: true, timeout: true });
    }, Math.max(1000, Math.min(timeoutMs || 120000, 600000)));
    const orig = w.resolve;
    w.resolve = (v) => {
      clearTimeout(timer);
      orig(v);
    };
  });
}

// ── 对端注册表 ───────────────────────────────────────

const peers = new Map(); // id -> {id, machine, host, port, lastSeen, source}

function notePeer(rec, source) {
  if (!rec || !rec.id) return;
  if (rec.id === identity.id) return;
  const prev = peers.get(rec.id) || {};
  peers.set(rec.id, {
    id: rec.id,
    machine: rec.machine || prev.machine || "",
    host: rec.host || prev.host || "",
    port: Number(rec.port) || prev.port || 0,
    version: rec.version || prev.version || "",
    os: rec.os || prev.os || "",
    source: source || prev.source || "",
    lastSeen: Date.now(),
  });
}

function peerList() {
  const now = Date.now();
  return [...peers.values()]
    .map((p) => ({ ...p, online: now - p.lastSeen < 30000 }))
    .sort((a, b) => Number(b.online) - Number(a.online));
}

// ── 组播心跳 ─────────────────────────────────────────

const selfRecord = () => ({
  v: PROTOCOL_VERSION,
  app: APP_TAG,
  id: identity.id,
  machine: identity.machine,
  hostname: hostname(),
  host: SELF_HOST,
  port: PORT,
  os: `${platform()} ${release()}`,
  user: (() => {
    try {
      return userInfo().username;
    } catch {
      return "unknown";
    }
  })(),
  profile: "workbuddy",
  version: "0.1.0",
  fingerprint: createHash("sha256").update(identity.id).digest("hex").slice(0, 16),
  requirePairing: false,
  ts: Date.now(),
});

function startMulticast() {
  const sock = createSocket({ type: "udp4", reuseAddr: true });

  sock.on("error", (err) => process.stderr.write(`[mesh-node] udp error: ${err.message}\n`));

  sock.on("message", (buf, rinfo) => {
    let msg;
    try {
      msg = JSON.parse(buf.toString("utf8"));
    } catch {
      return;
    }
    if (msg?.app !== APP_TAG || !msg.id) return;
    if (msg.id === identity.id) return;
    notePeer({ ...msg, host: msg.host || rinfo.address }, "multicast");
  });

  sock.bind(MULTICAST_PORT, () => {
    try {
      sock.addMembership(GROUP);
    } catch (err) {
      process.stderr.write(`[mesh-node] addMembership failed: ${err.message}\n`);
    }
    try {
      sock.setMulticastTTL(1);
      sock.setMulticastLoopback(true);
    } catch {}
    const beat = () => {
      const payload = Buffer.from(JSON.stringify(selfRecord()), "utf8");
      sock.send(payload, 0, payload.length, MULTICAST_PORT, GROUP, () => {});
    };
    beat();
    setInterval(beat, 5000).unref();
  });
}

// ── 复用本机 DSH 的 peer 表（免费发现）────────────────

async function probeLocalDsh() {
  if (!config.probeLocalDsh) return;
  try {
    const res = await fetch(`http://127.0.0.1:${config.localDshPort}/mesh/v1/peers`, {
      signal: AbortSignal.timeout(2000),
    });
    if (!res.ok) return;
    const body = await res.json();
    for (const p of body?.peers || []) {
      const ep = (p.endpoints || []).find((e) => e.host && e.port) || p;
      notePeer(
        { id: p.id, machine: p.machine, host: ep.host || p.host, port: ep.port || p.port, version: p.version, os: p.os },
        "local-dsh",
      );
    }
  } catch {}
}

// ── 派发 ─────────────────────────────────────────────

async function sendTask({ to, task, thread, timeoutMs = 15000 }) {
  const target = peerList().find((p) => p.machine === to || p.id === to);
  if (!target) {
    const err = new Error(`找不到对端：${to}`);
    err.code = "PEER_NOT_FOUND";
    throw err;
  }
  if (!target.host || !target.port) {
    const err = new Error(`对端 ${to} 缺少地址`);
    err.code = "PEER_NO_ADDRESS";
    throw err;
  }
  const payload = {
    id: `t-${randomBytes(6).toString("hex")}`,
    from: `${identity.machine}/workbuddy`,
    fromMachine: identity.machine,
    task,
    thread: thread || "",
  };
  const res = await fetch(`http://${target.host}:${target.port}/mesh/v1/task`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  return { ok: res.ok, status: res.status, sentTo: target.machine, id: payload.id, thread: payload.thread, response: body };
}

// ── HTTP 服务 ────────────────────────────────────────

function json(res, code, obj) {
  const buf = Buffer.from(JSON.stringify(obj), "utf8");
  res.writeHead(code, { "content-type": "application/json; charset=utf-8", "content-length": buf.length });
  res.end(buf);
}

function readBody(req, limit = 4 * 1024 * 1024) {
  return new Promise((resolve) => {
    let size = 0;
    const chunks = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > limit) {
        req.destroy();
        return resolve(null);
      }
      chunks.push(c);
    });
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      try {
        resolve(JSON.parse(raw));
      } catch {
        resolve(null);
      }
    });
    req.on("error", () => resolve(null));
  });
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  const path = url.pathname;
  const self = selfRecord();

  // ── 与 DSH mesh 协议兼容（让它把本节点当成合法对端）
  if (path === "/mesh/v1/health") return json(res, 200, { ok: true, ...self });
  if (path === "/mesh/v1/hello")
    return json(res, 200, { ok: true, ...self, requirePairingPinned: false, peerPaired: true });
  if (path === "/mesh/v1/peers") return json(res, 200, { ok: true, peers: peerList() });
  if (path === "/mesh/v1/tasks" && req.method === "GET")
    return json(res, 200, { ok: true, tasks: inbox.slice(-50).reverse() });
  if (path === "/mesh/v1/task" && req.method === "POST") {
    const body = await readBody(req);
    if (!body || typeof body.task !== "string" || !body.task.trim())
      return json(res, 400, { ok: false, error: "task 必填" });
    const r = addInbound(body);
    return json(res, 200, { ok: true, id: r.id, duplicate: r.duplicate });
  }

  // ── 本机 agent 用的接口
  if (path === "/health")
    return json(res, 200, { ok: true, id: identity.id, machine: identity.machine, host: SELF_HOST, port: PORT, inbox: inbox.length });

  if (path === "/peers") return json(res, 200, { ok: true, peers: peerList() });

  if (path === "/send" && req.method === "POST") {
    const body = await readBody(req);
    if (!body?.to || !body?.task) return json(res, 400, { ok: false, error: "to 与 task 必填" });
    try {
      const r = await sendTask(body);
      return json(res, 200, r);
    } catch (err) {
      return json(res, 502, { ok: false, error: err.message, code: err.code || "SEND_FAILED" });
    }
  }

  if (path === "/wait") {
    const thread = url.searchParams.get("thread") || "";
    const timeout = Number(url.searchParams.get("timeout") || 120000);
    const r = await waitFor(thread, timeout);
    return json(res, 200, r);
  }

  if (path === "/inbox") {
    const limit = Number(url.searchParams.get("limit") || 20);
    const unreadOnly = url.searchParams.get("unread") === "1";
    const list = inbox.filter((m) => (unreadOnly ? !m.consumed : true)).slice(-limit).reverse();
    return json(res, 200, { ok: true, count: list.length, messages: list });
  }

  json(res, 404, { ok: false, error: "not found" });
});

// ── 启动 ─────────────────────────────────────────────

loadInbox();
startMulticast();
probeLocalDsh().catch(() => {});
setInterval(() => probeLocalDsh().catch(() => {}), 30000).unref();

server.listen(PORT, "0.0.0.0", () => {
  process.stderr.write(
    `[mesh-node] listening ${SELF_HOST}:${PORT}  id=${identity.id}  machine=${identity.machine}  inbox=${inbox.length}\n`,
  );
});

for (const sig of ["SIGTERM", "SIGINT"]) {
  process.on(sig, () => {
    process.stderr.write(`[mesh-node] ${sig}, 退出\n`);
    server.close();
    process.exit(0);
  });
}
