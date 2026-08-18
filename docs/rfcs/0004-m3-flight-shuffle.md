# RFC 0004 — M3 Arrow Flight Shuffle Protocol

- **Status**: Implemented (2026-08-18)
- **Date**: 2026-08-18 (Draft) → 2026-08-18 (Implemented)
- **Author**: Pylon Working Group
- **Discussion**: Builds on RFC-0003 §5 (control + data plane) and M2 placeholder batch limitation

## 摘要

把 M2 的"coord 作为 gatherer"换成 **worker↔worker 的 Arrow Flight 流**。每个 worker 进程同时跑：1) gRPC control plane (pylon-proto::Worker) 收 coord 派发的 task；2) **Arrow Flight server** 把本 worker 产出的 RecordBatch 推给下游 worker。`ExchangeSinkOp` 把 batch 推到对端的 `DoExchange`，对端 `ExchangeSourceOp` 接到 batch 流回本 worker pipeline。

## 1. M2 留下的限制 + M3 要解的

| 问题 | M2 做法 | M3 解决 |
|---|---|---|
| 真 batch data 必须经过 coord | placeholder batch 凑合 (rows_total=10) | Flight peer-to-peer，coord 只做路由不持数据 |
| 多 stage query 不能跑 | 单 stage 后 coord 退出 | ExchangeSink/Source 把 N stages 串起来 |
| worker 之间不能直接通信 | 全部汇聚到 coord | Arrow Flight 对等 |
| Arrow IPC streaming codec 没接 | M3 required | 真实走 IPC streaming format |

## 2. Arrow Flight 速记

Apache Arrow Flight = gRPC + Arrow IPC streaming 的 binding。核心 RPC：

```
DoExchange(stream FlightData) returns (stream FlightData);     # 双工流
GetFlightData(FlightDescriptor) returns (FlightData);          # 单向拉
PutFlightData(stream FlightData) returns (PutResult);          # 单向推
```

每个 FlightData = 一段 Arrow IPC streaming message（schema / RecordBatch / EOS）。**DoExchange** 是 shuffle 的天然选择：producer 端 push batches，consumer 端长连接拉。

## 3. 进程拓扑 (M3)

```
┌─ pylon-coord ─┐                ┌─ pylon-worker-A ─┐
│ gRPC ctrl     │ TaskSpec       │ gRPC ctrl     ⇆  Flight server:50061 ⇆ Flight client (to own ExchangeSink)
│ HTTP API     │ TaskResponse   │                │                    │
│ Scheduler    │                │ Driver A: SeqScan → Filter → ExchangeSink (to peer)
│ + no data    │                │
└───────────────┘                └────────────────┘
                                  │
                                  │  Arrow IPC over HTTP/2
                                  ▼
                             ┌─ pylon-worker-B ─┐
                             │ gRPC ctrl     ⇆  Flight server:50062
                             │                │
                             │ Driver B: ExchangeSource → Filter → Project → out
                             └────────────────┘
```

**每个 worker 同时跑 2 个 server**：
- gRPC (pylon-proto::Worker) — 收 coord 派发的 task
- Arrow Flight (port from `--flight-port`) — 提供 peer-to-peer shuffle

## 4. Phase / Build 顺序

| Task | 1 行描述 | 状态 |
|---|---|---|
| task #1 | RFC-0004 (本文档) | ✅ |
| task #2 | arrow-flight dep + pylon-exchange crate + FlightService 实现 | TODO |
| task #3 | ExchangeSinkOp / ExchangeSourceOp in pylon-runtime/ops/ | TODO |
| task #4 | coord plan→ fragmenter 多 stage + 加 HashPartitionExchange 注入点 | TODO |
| task #5 | E2E: 2-worker `SELECT region, COUNT(*)` 跨 worker Flight shuffle | TODO |

## 5. Flight descriptor 协议

每条 peer-to-peer flight stream 用 `FlightDescriptor` 标识 (path 形式)：
```
descriptor = pylon://query/{qid}/stage/{sid}/task/{tid}/partition/{pid}
```

接收端 server 根据 path 决定是否接受该 stream（避免 worker 接到非法 source）。M3 实现时不严格校验，只 log + 拒绝 invalid path。

## 6. 数据格式

每条 FlightData message body = Arrow IPC streaming message（按 `arrow::ipc::reader::StreamReader` 协议）：

| byte | 内容 |
|---|---|
| 0xFF 0xFF 0xFF 0xFF + schema bytes | Schema message (M3: 一次 stream 一份) |
| 0xFF 0xFF 0xFF 0xFF + data | RecordBatch message (0..N 条) |
| 0xFF 0xFF 0xFF 0xFF + 0x00 0x00 0x00 0x00 | End-of-stream |

Producer：先写 schema message，再写多条 RecordBatch，最后写 EOS。
Consumer：从 stream 拿第一条 = schema message 缓存；之后每次一条 RecordBatch。

## 7. OpSpec 扩展 (proto)

新增两个 op name：
- `ExchangeSink`: 推 batch 到目标 worker `target_worker:target_partition`
- `ExchangeSource`: 从源 worker `source_worker:source_partition` 拉

OpSpec config 字段：

| op name | config key | config value | 含义 |
|---|---|---|---|
| ExchangeSink | target_worker | `host:port` | 接收端 worker flight address |
| ExchangeSink | target_partition | int | 接收端 partition id |
| ExchangeSource | source_worker | `host:port` | 来源端 worker flight address |
| ExchangeSource | source_partition | int | 来源端 partition id |

(worker 启动时 worker 注册 phase 把 `host:port` 上报到 coord；coord 派发 task 把 partition 路由信息填到 OpSpec config)

## 8. Op 实现 contract

```rust
struct ExchangeSinkOp {
    target: (SocketAddr, Partition),   // 远端
    downstream_schema: SchemaRef,
    shared_writer: Arc<Mutex<Option<StreamWriter<Vec<u8>>>>>,
    finished: bool,
}

#[async_trait]
impl PipelineOp for ExchangeSinkOp {
    fn name(&self) -> &'static str { "ExchangeSink" }
    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        // 1. 拿到 writer（如未打开，建立 Flight DoExchange client
        //    → 连接 target_worker:port → 拿到 stream）
        // 2. writer.write(&batch)?
    }
    async fn get_output(&mut self) -> Result<Option<RecordBatch>> { Ok(None) }  // sink 不输出
    async fn is_finished(&self) -> bool { self.finished && writer.is_none() }
    // is_blocked 返回 future: pending 的 flight connect/send
}
```

```rust
struct ExchangeSourceOp {
    source: (SocketAddr, Partition),
    upstream_schema: Option<SchemaRef>,
    reader: Option<StreamReader<...>>,
}

#[async_trait]
impl PipelineOp for ExchangeSourceOp {
    fn name(&self) -> &'static str { "ExchangeSource" }
    async fn needs_input(&self) -> bool { false }   // source op
    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        // 1. 第一次: open Flight DoExchange client conn, read schema msg
        // 2. 之后: read next RecordBatch from stream
        // 3. None 表示 EOS
    }
}
```

## 9. Coordinator 路由信息

每 worker 启动时同时开 gRPC (已知) 和 Flight server。注册时上报：
- `flight_addr: String` — `host:port` 

coord 维护 `Worker { grpc_addr, flight_addr, ... }`：

```rust
struct WorkerRegistration {
    worker_id: WorkerId,
    grpc_addr: SocketAddr,        // pylon-proto::Worker service
    flight_addr: String,           // "host:port" for Arrow Flight (shuffle target)
    capacity: WorkerCapacity,
}
```

当 fragmenter 决定 stage N 输出到 stage M 时：
```rust
ExchangeSpec {
    kind: ExchangeKind::PARTITIONED,  // or BROADCAST / GATHER
    target_worker: stage_M's worker.flight_addr + ":" + partition_id,
    target_partition: partition_id,
    source_partition: stage_N's task's partition_id,
}
```

放到 stage N task 的 ExchangeSinkOp config + stage M task 的 ExchangeSourceOp config。

## 10. 失败处理 (M3 简化)

- worker 进程 crash → 整个 worker 的所有 in-flight task 视为失败；coord 取消所有依赖此 worker 的下游 task；query 整体失败（**不重试**，M4 加 FTE）
- Flight stream 断开 → sink 检测到 write 错误 → 标 task 失败
- Flight stream 卡死 → 用 tokio timeout（默认 30s）→ 标 task 失败

## 11. Out-of-scope (M4 解决)

- ✗ FTE + shuffle output 持久化到对象存储 — M4 / RFC-0006
- ✗ Coordinator HA — M5
- ✗ 自适应 batch size — 默认 4 MiB
- ✗ TLS / 鉴权 — M5

## 12. 实施 DoD

| 项 | 验收 | 实际 |
|---|---|---|
| `pylon-exchange` crate 实现 FlightServer trait + WorkerFlightService impl | `cargo build -p pylon-exchange` 通过 | ✅ `flight_rpc.rs::FlightServerImpl` |
| `ExchangeSinkOp` 把本 worker batch 推到 Arrow Flight stream | unit test | ✅ `tests/exchange_test.rs` |
| `ExchangeSourceOp` 从 Arrow Flight stream 拉 batch 回 pipeline | unit test | ✅ 同上 |
| 2-worker E2E: `SELECT region, COUNT(*) GROUP BY region` 跨 worker 走 Flight, 结果准确 | integration test | ✅ `tools/e2e/two_worker_smoke.sh` (改成 `SELECT name, COUNT(*) FROM sample GROUP BY name`，sample 列名不同) |
| M3 阶段 `git commit` 干净，5+ 个新单元测试通过 | final | ✅ 38+ 个新单测，91 passing (33 suites) |

## 13. 决策 checklist

- [x] DEC-001 控制平面仍 gRPC (RFC-0003) — `crates/pylon-proto/proto/pylon.proto` 的 `Worker.OpenSession` bidi
- [x] DEC-002 数据平面 worker↔worker Arrow Flight (本 RFC) — `crates/pylon-runtime/src/ops/exchange.rs::ExchangeSinkRpc` 走 tonic `DoExchange`
- [x] DEC-003 IPC streaming format + DoExchange RPC — `crates/pylon-exchange/src/flight_rpc.rs::FlightServerImpl::do_exchange` + `crates/pylon-exchange/src/flight_client.rs::PylonFlightClient`
- [x] DEC-004 每 worker 双 server (gRPC + Flight) 同进程 — `crates/pylon-worker/src/main.rs` 同进程跑 `WorkerServer` + `FlightServiceServer`
- [x] DEC-005 默认 partition 16 + 默认 batch 4 MiB，与 RFC-0003 决策一致 — M3 B-3.5 first cut 改用 `default_partition_count=2` for the 2-worker demo; 4 MiB batch default 保留

## 14. Sign-off

Date: 2026-08-18 (Implemented)

落地拆成 12 个 commit 推到 `codex/m3-hash-partition-exchange`：
- A1-1..A1-5 + A1 rollup (`Logical::Aggregate` + `HashAggregateOp` + worker wiring + 1-stage E2E)
- A2-1..A2-2 + A2 rollup (`Fragmenter` post-order + `HashPartitionExchange` + 2-stage in-process E2E)
- B-1 (`Discovery` + `RegisterWorker` proto + worker Flight server)
- B-2 (`ExchangeSinkRpc` over tonic `DoExchange`)
- B-3 (2-worker smoke E2E script)
- B-3.5 gap1 (worker 真 Arrow IPC 编码 + coord decode)
- B-3.5 gap2 (coord dispatch 切 `Fragmenter::fragment_with_workers` + 真 2-worker shuffle E2E)

Sign-off packet: [docs/notes/m3-status.md](../notes/m3-status.md)。End-to-end 验证在 `tools/e2e/two_worker_smoke.sh` —— 2 个 OS 进程 + 1 coord + 真 Arrow Flight `DoExchange` 跑通 `SELECT name, COUNT(*) FROM sample GROUP BY name` 端到端。

下一步 (M4)：FTE (写 Arrow IPC stream 到 S3) + Spill + 容错。RFC-0006 待写。

## 15. M3 Tail — exchange unification (post-sign-off cleanup)

The first cut described above kept two parallel producer shapes:
`ExchangeSinkOp` (in-process `PylonFlightService::push`) for same-worker
shuffles and `ExchangeSinkRpc` (tonic `DoExchange`) for cross-worker.
This forced the fragmenter to branch on `worker_flight_addrs.is_empty()`,
gave the worker factory two branches, and spread same-worker vs
cross-worker logic across the runtime and worker crates.

The M3-tail cleanup (`docs/roadmap/m3-tail-exchange-unify.md`) collapsed
the two paths:

- **PR1 (B3)** — moved `target_flight_addrs` computation from the
  fragmenter to the coord dispatcher. The dispatcher is now the
  authoritative source for stage1 partition → worker assignment; the
  fragmenter's placeholder is overwritten before any task reaches the
  worker.
- **PR2 (B1+B2)** — deleted `ExchangeSinkOp`, `fragment_multi_stage`,
  and the worker `"ExchangeSink"` factory branch. The fragmenter now
  emits exactly one op shape (`ExchangeSinkRpc`); the worker has one
  factory branch. Same-worker fan-out is naturally expressed as
  `target.flight_addr == local flight_addr` — a true loopback gRPC
  call through the local `FlightServerImpl`.
- **B8** — removed the `// M3 first cut` / `// B-2 routing` markers
  from `fragment.rs`, dropped the dead `build_stage0_ops` /
  `build_stage1_ops` / `aggregate_results` helpers from
  `pylon-coord/src/bin/pylon-coord.rs`, and updated the
  `aggregate_2stage_e2e_test` to drive the unified Flight path
  against a loopback server.

The unified path is one producer (`ExchangeSinkRpc`) + one consumer
(`ExchangeSourceOp`); the dispatcher is the single authority for
placement; the fragmenter owns plan-shape only. There is no longer a
distinction between "in-process" and "RPC" at the operator level —
only at the address level (`flight_addr`).

The M3-tail #1 — replacing the `sleep(3)` coord-side polling with a
real `TaskDone` ack — is the remaining loose end. Once that lands, the
last `tokio::time::sleep` barriers in tests can be replaced with
proper join handles.
