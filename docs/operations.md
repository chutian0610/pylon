# Operations

Running and testing Pylon: binaries, environment variables, and the
chaos/FTE tooling.

## Binaries

| binary | role | key flags / env |
|---|---|---|
| `pylon-coord` | HTTP `:8080` + gRPC `:9090` (fleet control plane) | `PYLON_HTTP_PORT`, `PYLON_GRPC_PORT`, `PYLON_TASK_MEMORY_BUDGET_BYTES` |
| `pylon-worker` | gRPC + Arrow Flight server + pipeline runner | `--flight-addr`, `--grpc-addr`, `--spill-root`, `PYLON_FLIGHT_ADDR`, `PYLON_GRPC_ADDR`, `PYLON_SPILL_ROOT`, `PYLON_COORDINATOR` |

Both read `RUST_LOG` (`pylon=info` is a good default).

## Environment variables

| variable | side | meaning |
|---|---|---|
| `PYLON_HTTP_PORT` / `PYLON_GRPC_PORT` | coord | HTTP API / gRPC fleet ports |
| `PYLON_TASK_MEMORY_BUDGET_BYTES` | coord → task | per-task aggregate byte budget; small values force the spill → `TASK_STALLED` checkpoint path |
| `PYLON_FLIGHT_ADDR` / `PYLON_GRPC_ADDR` | worker | Arrow Flight / gRPC listen addresses (`:0` = kernel-assigned) |
| `PYLON_SPILL_ROOT` | worker | spill + persisted-input root (default: `<tmp>/pylon-worker-pid<pid>`) |
| `PYLON_COORDINATOR` | worker | coord gRPC URL |

## Sample data

```bash
cargo run -p gen-sample-data -- --rows 100000 --groups 100000
#   --rows N      total input rows
#   --groups G    distinct `name` values (aggregate output cardinality)
#   --out PATH    default data/sample.parquet
```

## Chaos / FTE tooling

| script | what it does |
|---|---|
| `tools/e2e/two_worker_smoke.sh` | 2-worker cross-process Flight shuffle smoke E2E |
| `tools/chaos/stall_retry_e2e.sh` | tiny budget forces spill → `TASK_STALLED` checkpoints → clean DONE |
| `tools/chaos/kill_worker_e2e.sh [rounds] [settle_s]` | SIGKILL a worker around the query; asserts a bounded terminal state |
| `tools/chaos/fte_kill_e2e.sh [rounds]` | mid-task kill; asserts input-log replay produces the exact result |
| `tools/chaos/s8_signoff_e2e.sh` | M4 sign-off: uninterrupted baseline vs mid-task-kill run must match exactly |
| `tools/chaos/random_kill.sh [rounds] [max_delay]` | kill loop with randomized timing |

Scale knobs for the sign-off: `S8_ROWS` (input rows) and `S8_GROUPS`
(distinct aggregate keys). Defaults: 20M × 1M (~15 min). The RFC's
literal 1B-row run is the same script parameterized and takes hours on
a single host.

## Notes

- FTE input logs live under `<worker spill_root>/pylon-input/` and
  are write-ahead: every batch is appended before it becomes visible
  to consumers. Orphaned logs (crashed runs) can be reaped with
  `S3SpillStore::delete_prefix` or a bucket lifecycle rule.
- A `TASK_STALLED` checkpoint is emit-and-continue: the task keeps
  running after acking; the coord re-dispatches from the checkpoint
  only if the worker dies.
