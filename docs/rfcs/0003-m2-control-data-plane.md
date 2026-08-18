# RFC 0003 — M2 Control Plane + Data Plane Spec

- **Status**: Draft
- **Date**: 2026-08-18
- **Author**: Pylon Working Group
- **Discussion**: Builds on RFC-0001 §ADR-001 and RFC-0002 §4 hierarchy

## 摘要

定义 M2 阶段的多进程拓扑：1 个 coord + N 个 worker 之间的**控制平面**（gRPC）和**数据平面**（本阶段双向往返 coord；M3 改 Arrow Flight）。锁定协议、消息体结构、失败恢复语义。

## 1. 拓扑

```
┌───────────────┐                  ┌───────────────┐
│ HTTP client   │ HTTP (axum+JSON) │               │
│ curl / JDBC   │ ──────────────► │  pylon-coord  │ ──gRPC──► pylon-worker-A
└───────────────┘                  │               │ ──gRPC──► pylon-worker-B
                                   │               │ ──gRPC──► pylon-worker-C
                                   │               │
                                   └───────────────┘
                                          ▲
                                          │ gRPC bidi stream (results)
                                          │
                                          ● pylon-workers send RecordBatch
                                            back to coord; coord aggregates
                                            for now (M3: peer-to-peer Flight)
```

控制平面 = coord ↔ worker（gRPC 单向/双向流）
数据平面 = M2: worker → coord（gRPC stream）；M3: peer-to-peer Arrow Flight（worker → worker）

## 2. 控制平面协议

### 2.1 Worker 注册

```
rpc Register (RegisterRequest) returns (RegisterResponse);
```

字段：
- `socket: string` — worker 监听 Arrow Flight 的 host:port (M3)
- `flight_port: u32` — 同上,分离字段以便后续扩展
- `max_drivers: u32`
- `max_memory_bytes: u64`

返回：`worker_id: u64`

### 2.2 Coord 派发任务

```
rpc StreamTasks (stream TaskRequest) returns (stream TaskResponse);
```

bidirectional streaming：
- Coord → Worker：`TaskRequest { task_spec: TaskSpec }`
- Worker → Coord：`TaskResponse { task_id, state, batches: [RecordBatch] | Arrow IPC stream }`

每个 task = 1 条 `TaskRequest` + 多条 `TaskResponse`（每批一次，最后一条 Done/Failed）。
Coord 关闭 stream = 取消任务（worker 检测 stream 关闭即 cancel driver）。

### 2.3 失败通知

Coord 检测到 worker 长时间不响应 → 取消该 worker 所有未完成 task，状态置 Failed，下游 task 也 cancel。

## 3. TaskSpec over wire (proto)

```proto
message TaskSpec {
  uint64 id = 1;
  uint64 query_id = 2;
  uint64 stage_id = 3;
  uint32 partition = 4;
  Fragment fragment = 5;
  repeated ExchangeSpec sources = 6;
  repeated ExchangeSpec sinks = 7;
  uint64 memory_budget_bytes = 8;
}

message Fragment {
  repeated OpSpec ops = 1;
  Distribution distribution = 2;
}

message OpSpec {
  string name = 1;                 // op 名: SeqScan / Filter / Project / HashAggregate / HashPartitionExchange / ...
  map<string, string> config = 2;  // 简单 key-value 配置
}

enum Distribution {
  SINGLE = 0;
  PARTITIONED = 1;
  BROADCAST = 2;
}

message ExchangeSpec {
  ExchangeKind kind = 1;
  string target_worker = 2;        // "host:port"
  uint32 target_partition = 3;
  uint32 source_partition = 4;
}

enum ExchangeKind {
  PARTITIONED = 0;
  BROADCAST = 1;
  GATHER = 2;
  LOCAL = 3;
}
```

## 4. RecordBatch wire（数据平面）

每条 RecordBatch 通过 Arrow IPC streaming format 序列化（`arrow::ipc::writer::StreamWriter`）：

```
┌──────────────────────────────────────────────────────┐
│ Schema message (连续流共享,只在第一次发)              │
├──────────────────────────────────────────────────────┤
│ RecordBatch message body                             │
├──────────────────────────────────────────────────────┤
│ Continuation token (0xFFFFFFFF 表示流结束)            │
└──────────────────────────────────────────────────────┘
```

M2: 整段任务结束或每批 batch 之间用 stream message 切。Coord 收到 IPC stream → arrow::ipc::reader 解码为 RecordBatch。

## 5. M2 第一阶段双向往返 coord 数据通路

**简化决策**：M2 不引入 Arrow Flight peer-to-peer；worker 把产生的 batch 流回 coord，coord 在内存中聚合（M2 多 stage 用 coord-as-gatherer）。

```
Stage 0: worker-A → SeqScan batches   ─gRPC stream→ coord
         worker-B → SeqScan batches   ─gRPC stream→ coord
         worker-C → SeqScan batches   ─gRPC stream→ coord
                  ▼
         coord 内存里拼接 (Round-Robin 或 record-level concat)
                  ▼
         coord 把"已合并的" stream 转给 final-stage worker
                  ▼
         worker-A (最终聚合, single stage) → 写 → gRPC stream → coord → HTTP 客户端
```

**理由**：
- M2 的 DoD 是 "coord+workers 处理一个跨进程的 query"
- 跨进程 shuffle 是 M3+ 范围（RFC-0004）
- Coord-as-gatherer 让多阶段语义能在 M2 跑得通

**缺点**：
- M2 极限吞吐量被 coord 单节点限制
- M3 用 Arrow Flight worker↔worker 解决

## 6. HTTP API

```
POST /v1/query
Content-Type: application/json
{ "sql": "SELECT ...", "timeout_ms": 60000 }

→ 202 Accepted
{ "query_id": "q-..." }


GET /v1/query/{id}
→ 200 OK
{ "query_id": "...", "state": "running|done|failed",
  "rows_returned": [...],   // up to N rows preview
  "schema": { ... } }


GET /v1/workers
→ 200 OK
{ "workers": [...] }
```

M2: 接受 + 返回，不带 auth；M5+ 加 JWT。

## 7. 失败模型（M2 简化版）

| 异常 | 行为 |
|---|---|
| Worker 进程 panic | Coordinator 30s 内检测心跳丢失；该 worker 上 in_flight tasks 标记 Failed；query 标记 Failed |
| 单 task failed | 通过 gRPC stream 返回 `{state: Failed}`；coord 取消所有下游；query 标 Failed |
| Coord 进程 panic | Workers 超时（gRPC stream 错误）；users 看到 `POST /query` 失败 |
| 网络分区 | 同 worker panic，但 coord 给 30s grace 后再决定（M4 FTE 重做） |

**M2 没有 FTE retry**。失败 = query 失败。这是简化决策。

## 8. Out-of-scope（M2 不做）

- ✗ Arrow Flight peer-to-peer（推到 M3 / RFC-0004）
- ✗ FTE + retry（推到 M4 / RFC-0006）
- ✗ Coordinator HA / Raft（推到 M5）
- ✗ Auth / TLS / 鉴权
- ✗ Substrait cross-engine IO

## 9. 决策清单

| ID | 决策 |
|---|---|
| DEC-001 | 控制平面协议 = gRPC (tonic + prost)，workspace 新增 pylon-proto crate |
| DEC-002 | 数据平面 = worker → coord（M2 简化），M3 转 Arrow Flight |
| DEC-003 | HTTP API = axum + JSON，2 端点：POST /query + GET /query/:id |
| DEC-004 | 失败 = query 失败 (无 FTE)，M4 重做 |
| DEC-005 | 1 coordinator + 多 worker，每个 worker 跑 1 个 binary `pylon-worker` |
| DEC-006 | Coordinator binary 名为 `pylon-coord` |
| DEC-007 | 默认端口：coord gRPC = 9090；coord HTTP = 8080（暂不冲突） |

## 10. 实施 checklist（M2 session）

- [ ] 添加 tonic + prost + axum 到 workspace deps
- [ ] 创建 pylon-proto crate，proto 文件 + build.rs
- [ ] pylon-coord binary: HTTP API + gRPC server
- [ ] pylon-worker binary: gRPC client + task runner
- [ ] 端到端：1 coord + 2 workers，SQL 走通
- [ ] TPC-H Q1 跨 worker 测试
- [ ] 写 RFC-0004 (Arrow Flight - M3)
- [ ] commit M2 milestone

## Sign-off

Date: 2026-08-18
