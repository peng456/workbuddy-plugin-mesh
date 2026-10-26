#!/usr/bin/env node
/**
 * mesh-mcp — MCP 门面（stdio）
 *
 * 只做三件事：
 *   1. 跟 WorkBuddy 说 MCP 协议（JSON-RPC over stdin/stdout）
 *   2. 把工具调用转成对 mesh-node 的本地 HTTP 请求
 *   3. 节点没起来就顺手拉起来（detached，不随本进程退出）
 *
 * 它自己**不持有任何状态**——状态全在 mesh-node 里，
 * 所以本进程被宿主回收也不影响收件箱。
 */

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";

const HERE = dirname(fileURLToPath(import.meta.url));
const NODE_SCRIPT = join(HERE, "mesh-node.mjs");
const PORT = Number(process.env.MESH_NODE_PORT || 45930);
const BASE = `http://127.0.0.1:${PORT}`;

const SUPPORTED_PROTOCOLS = ["2025-06-18", "2025-03-26", "2024-11-05"];

function log(...args) {
  process.stderr.write(`[mesh-mcp] ${args.join(" ")}\n`);
}

// ── 节点保活 ─────────────────────────────────────────

async function nodeAlive() {
  try {
    const res = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(800) });
    return res.ok;
  } catch {
    return false;
  }
}

let spawning = null;

async function ensureNode() {
  if (await nodeAlive()) return true;
  if (!spawning) {
    spawning = (async () => {
      log("节点未运行，尝试拉起");
      try {
        const child = spawn(process.execPath, [NODE_SCRIPT], {
          detached: true,
          stdio: "ignore",
          env: process.env,
        });
        child.unref();
      } catch (err) {
        log(`拉起失败：${err.message}`);
      }
      for (let i = 0; i < 25; i++) {
        await new Promise((r) => setTimeout(r, 200));
        if (await nodeAlive()) return true;
      }
      return false;
    })().finally(() => {
      spawning = null;
    });
  }
  return spawning;
}

// ── 节点调用 ─────────────────────────────────────────

async function callNode(path, { method = "GET", body, timeoutMs = 10000 } = {}) {
  const ok = await ensureNode();
  if (!ok) throw new Error("mesh-node 未能启动（端口被占用或权限问题）");
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return { ok: false, error: `节点返回非 JSON：${text.slice(0, 200)}` };
  }
}

// ── 工具定义 ─────────────────────────────────────────

const TOOLS = [
  {
    name: "mesh_peers",
    description:
      "列出局域网内可用的 mesh 对端（其他机器上的 agent）。返回每台机器的名字、地址、版本、在线状态。派发任务前先用它确认对端在线。",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "mesh_send",
    description:
      "给另一台机器上的 agent 派一条任务。立即返回（对方受理即回），不会等对方跑完。若想等回复，用同一个 thread 再调 mesh_wait。",
    inputSchema: {
      type: "object",
      properties: {
        to: { type: "string", description: "对端机器名（mesh_peers 里的 machine）或 id" },
        task: { type: "string", description: "要对方做的事，写成一段自然语言指令" },
        thread: { type: "string", description: "可选。会话线索标识，同一来回复用同一个值，用于等待时匹配回执" },
      },
      required: ["to", "task"],
      additionalProperties: false,
    },
  },
  {
    name: "mesh_wait",
    description:
      "阻塞等待对端回执。等到匹配 thread 的消息就返回；超时返回 timeout=true（不报错）。timeout 按对端跑一轮的时间估，简单问答 20~30 秒，需要跑命令 60~120 秒。",
    inputSchema: {
      type: "object",
      properties: {
        thread: { type: "string", description: "只等这个 thread 的回执；留空则等任意新消息" },
        timeout: { type: "number", description: "最长等待秒数，默认 120，上限 600" },
      },
      additionalProperties: false,
    },
  },
  {
    name: "mesh_inbox",
    description: "查看收件箱里对端发来的消息（默认最近 20 条，可只看未读）。不消耗等待，随时可查。",
    inputSchema: {
      type: "object",
      properties: {
        limit: { type: "number", description: "返回条数，默认 20" },
        unread: { type: "boolean", description: "只看未读" },
      },
      additionalProperties: false,
    },
  },
];

// ── 工具实现 ─────────────────────────────────────────

async function runTool(name, args = {}) {
  if (name === "mesh_peers") {
    const r = await callNode("/peers");
    const list = (r.peers || []).filter((p) => p.online);
    if (list.length === 0) return "没有发现在线的 mesh 对端。确认对端机器上的 DSH/mesh 正在运行，且与本机同一网段。";
    return list
      .map(
        (p) =>
          `· ${p.machine}  ${p.host}:${p.port}  ${p.online ? "在线" : "离线"}  ` +
          `${p.version ? `v${p.version} ` : ""}${p.source ? `(来源:${p.source})` : ""}`,
      )
      .join("\n");
  }

  if (name === "mesh_send") {
    const r = await callNode("/send", {
      method: "POST",
      body: { to: args.to, task: args.task, thread: args.thread },
      timeoutMs: 20000,
    });
    if (!r.ok) return `派发失败：${r.error || JSON.stringify(r)}`;
    return `已派发给 ${r.sentTo}\n  id: ${r.id}\n  thread: ${r.thread || "(无)"}\n  对端应答: ${JSON.stringify(r.response)}`;
  }

  if (name === "mesh_wait") {
    const timeout = Math.max(1, Math.min(Number(args.timeout || 120), 600));
    const qs = new URLSearchParams({ timeout: String(timeout * 1000) });
    if (args.thread) qs.set("thread", args.thread);
    const r = await callNode(`/wait?${qs.toString()}`, { timeoutMs: timeout * 1000 + 5000 });
    if (r.timeout) return `等了 ${timeout} 秒，没有收到匹配的回执。消息可能还在对端处理中，稍后可再用 mesh_inbox 查。`;
    const m = r.message || {};
    return `收到来自 ${m.fromMachine || m.from} 的消息（thread: ${m.thread || "无"}）:\n\n${m.task}`;
  }

  if (name === "mesh_inbox") {
    const qs = new URLSearchParams({ limit: String(args.limit || 20) });
    if (args.unread) qs.set("unread", "1");
    const r = await callNode(`/inbox?${qs.toString()}`);
    const msgs = r.messages || [];
    if (msgs.length === 0) return "收件箱为空。";
    return msgs
      .map((m) => `[${m.receivedAt}] ${m.fromMachine || m.from} (thread:${m.thread || "-"})\n${m.task}`)
      .join("\n\n---\n\n");
  }

  throw new Error(`未知工具：${name}`);
}

// ── JSON-RPC 主循环 ──────────────────────────────────

function send(obj) {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

function reply(id, result) {
  send({ jsonrpc: "2.0", id, result });
}

function replyError(id, code, message) {
  send({ jsonrpc: "2.0", id, error: { code, message } });
}

async function handle(msg) {
  const { id, method, params } = msg;

  if (method === "initialize") {
    const wanted = params?.protocolVersion;
    const version = SUPPORTED_PROTOCOLS.includes(wanted) ? wanted : SUPPORTED_PROTOCOLS[0];
    reply(id, {
      protocolVersion: version,
      capabilities: { tools: {} },
      serverInfo: { name: "mesh-mcp", version: "0.1.0" },
      instructions:
        "局域网跨机 agent 通信。mesh_peers 看在线对端；mesh_send 派任务；用同一 thread 调 mesh_wait 等回执。",
    });
    return;
  }

  if (method === "notifications/initialized" || method === "initialized") return;

  if (method === "ping") {
    reply(id, {});
    return;
  }

  if (method === "tools/list") {
    reply(id, { tools: TOOLS });
    return;
  }

  if (method === "tools/call") {
    const name = params?.name;
    const args = params?.arguments || {};
    try {
      const text = await runTool(name, args);
      reply(id, { content: [{ type: "text", text }] });
    } catch (err) {
      reply(id, { content: [{ type: "text", text: `调用失败：${err.message}` }], isError: true });
    }
    return;
  }

  if (id !== undefined && id !== null) {
    replyError(id, -32601, `方法未实现：${method}`);
  }
}

const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

let inflight = 0;
let closed = false;

rl.on("line", (line) => {
  const trimmed = line.trim();
  if (!trimmed) return;
  let msg;
  try {
    msg = JSON.parse(trimmed);
  } catch {
    log(`无法解析：${trimmed.slice(0, 120)}`);
    return;
  }
  inflight++;
  handle(msg)
    .catch((err) => {
      log(`处理异常：${err.message}`);
      if (msg?.id !== undefined) replyError(msg.id, -32603, err.message);
    })
    .finally(() => {
      inflight--;
      if (closed && inflight === 0) process.exit(0);
    });
});

rl.on("close", () => {
  // stdin 关闭时不要立刻退出：可能还有在途的工具调用（尤其是 mesh_wait）
  closed = true;
  if (inflight === 0) process.exit(0);
});

log(`started, node=${BASE}`);
ensureNode().catch(() => {});
