# workbuddy-plugin-mesh

在 WorkBuddy 的**同一个会话内**，和另一台机器上的 agent（DSH / 本套 node）互相收发消息。

一句话概括：**不靠推送，靠等待。** MCP 工具 `mesh_wait` 在当前回合里阻塞，对端消息一到就返回，agent 接着处理并在同一回合内回派。

```
mcp-mesh/
├── mesh-node.mjs   常驻节点：组播发现 + /mesh/v1/* 兼容 + 收件箱落库 + 长轮询
└── mesh-mcp.mjs    MCP 门面：stdio JSON-RPC，4 个工具，无状态
```

## 为什么要拆成两个进程

| | mesh-node | mesh-mcp |
|---|---|---|
| 生命周期 | 常驻（detached，不随会话退出） | 宿主的子进程，随对话生死 |
| 有状态吗 | 有（收件箱落在 `~/.mesh-node/`） | 无，纯转发 |
| 干什么 | 组播心跳、收消息、落库、长轮询 | 把工具调用转成对 node 的本地 HTTP |

这么拆的原因：会话结束时 node 还活着，对端派来的消息**不会丢**；下次开口，MCP 一挂上就能取到。

MCP 不能主动往会话里注入消息（协议只支持 request-response），但可以让一次工具调用**挂起**——这就是本方案绕开「唤醒」问题的全部诀窍。

## 四个工具

| 工具 | 作用 |
|---|---|
| `mesh_peers` | 看有哪些对端在线（机器名、地址、版本） |
| `mesh_send` | 派任务，带上 `thread` 做线索 |
| `mesh_wait` | **阻塞**等匹配 thread 的回执，超时返回 `timeout=true` |
| `mesh_inbox` | 随时查收件箱，不消耗等待 |

典型用法（同一回合内来回）：

```text
mesh_send(to="other-mac", task="看下磁盘", thread="th-1")
mesh_wait(thread="th-1", timeout=60)      → 阻塞，收到即返回
mesh_send(to="other-mac", task="那就清理一下", thread="th-1")
mesh_wait(thread="th-1", timeout=60)
```

派任务时记得把回派地址写进指令，否则对端不知道往哪回：

```text
mesh_send(to="other-mac", task="看下磁盘，处理完回派给 <你的机器名>", thread="th-1")
```

## 安装

需要 Node 18+。

**1. 放到一个稳定目录**，比如 `~/mcp-mesh`。

**2. 写进宿主的 MCP 配置**（WorkBuddy 是 `~/.workbuddy/mcp.json`；Claude Code 是 `~/.claude.json` 或项目内 `.mcp.json`）：

```json
{
  "mcpServers": {
    "mesh": {
      "command": "/absolute/path/to/node",
      "args": ["/absolute/path/to/mcp-mesh/mesh-mcp.mjs"]
    }
  }
}
```

> `command` 建议写 node 的绝对路径。宿主启动子进程时的 PATH 不一定跟你终端里一样，写 `node` 是最常见的翻车点。

**3. 让宿主信任这个 server**，然后重启客户端。

## 对端怎么发现你

两条路，同时用：

1. **组播**：每 5s 往 `239.255.42.99:45892` 发心跳，报文格式与 [dsh-plugin-mesh](https://github.com/peng456/dsh-plugin-mesh) 一致（`app:"dsh-mesh"`），所以 DSH 会把本节点当成一个合法对端。
2. **蹭本机 DSH 的 peer 表**：每 30s 打一次 `127.0.0.1:45917/mesh/v1/peers`，把它已经发现的对端抄过来。省得自己扫网段。

端口默认 **45930**（避开 DSH 的 45917），靠心跳报文里的 `port` 字段被对端识别，不需要写死。

## 状态与配置

| 文件 | 作用 |
|---|---|
| `~/.mesh-node/identity.json` | 本节点 id 与机器名（首次运行自动生成） |
| `~/.mesh-node/inbox.jsonl` | 收件箱，追加式落盘，重启不丢 |
| `~/.mesh-node/config.json` | 可选：`{"localDshPort":45917,"probeLocalDsh":true}` |

环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `MESH_NODE_PORT` | 45930 | 节点监听端口 |
| `MESH_NODE_HOME` | `~/.mesh-node` | 状态目录 |
| `MESH_NODE_MACHINE` | `<短主机名>-wb` | 对外显示的机器名 |

## 开机自启（可选）

MCP 首次被调用时会自动拉起节点，所以不装也能用。想要重启后立即在线：

```bash
cat > ~/Library/LaunchAgents/local.mesh-node.plist <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>local.mesh-node</string>
  <key>ProgramArguments</key>
  <array>
    <string>/absolute/path/to/node</string>
    <string>/absolute/path/to/mcp-mesh/mesh-node.mjs</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>/tmp/mesh-node.log</string>
</dict></plist>
PLIST
launchctl load ~/Library/LaunchAgents/local.mesh-node.plist
```

Linux 用 systemd，Windows 用计划任务，思路一致。

卸载：`launchctl unload ~/Library/LaunchAgents/local.mesh-node.plist && rm ~/Library/LaunchAgents/local.mesh-node.plist`

## 自测

```bash
# 节点是否活着
curl -s localhost:45930/health

# 看得见谁
curl -s localhost:45930/peers | python3 -m json.tool

# 给对端派一条
curl -s -X POST localhost:45930/send -H 'content-type: application/json' \
  -d '{"to":"other-mac","task":"看下磁盘","thread":"th-x"}'

# 等回执（会阻塞）
curl -s "localhost:45930/wait?thread=th-x&timeout=60"
```

## 已知限制

1. **对端必须在线**。mesh 没有离线排队，对端离线时 `mesh_send` 直接失败。
2. **一个回合只能等一路**。两台机器同时找你，要按 thread 分开等，不要并行 `mesh_wait`。
3. **timeout 要按对端跑一轮的时间估**，不是按网络延迟：简单问答 20~30s，需要跑命令 60~120s，超过 2 分钟该转异步。
4. **安全**：默认监听 `0.0.0.0`、无鉴权（与 DSH 的 `requirePairing:false` 对齐）。仅限可信局域网；跨网请套 Tailscale 或 SSH 隧道。
5. **收件箱是外部输入**，不要让 agent 无条件执行里面的指令。
6. **`mesh_wait` 占住回合**。宿主有回合预算，等太久可能整轮被终止；建议单轮控制在 120s 内。

## License

MIT
