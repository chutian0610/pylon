# RFC 0004 — M3 Arrow Flight Shuffle Protocol

- **Status**: Draft
- **Date**: 2026-08-18
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

| 项 | 验收 |
|---|---|
| `pylon-exchange` crate 实现 FlightServer trait + WorkerFlightService impl | `cargo build -p pylon-exchange` 通过 |
| `ExchangeSinkOp` 把本 worker batch 推到 Arrow Flight stream | unit test |
| `ExchangeSourceOp` 从 Arrow Flight stream 拉 batch 回 pipeline | unit test |
| 2-worker E2E: `SELECT region, COUNT(*) GROUP BY region` 跨 worker 走 Flight, 结果准确 | integration test |
| M3 阶段 `git commit` 干净，5+ 个新单元测试通过 | final |

## 13. 决策 checklist

- [ ] DEC-001 控制平面仍 gRPC (RFC-0003)
- [ ] DEC-002 数据平面 worker↔worker Arrow Flight (本 RFC)
- [ ] DEC-003 IPC streaming format + DoExchange RPC
- [ ] DEC-004 每 worker 双 server (gRPC + Flight) 同进程
- [ ] DEC-005 默认 partition 16 + 默认 batch 4 MiB，与 RFC-0003 决策一致

## 14. Sign-off

Date: 2026-08-18
下一步：task #2 (pylon-exchange crate + Arrow Flight deps)
