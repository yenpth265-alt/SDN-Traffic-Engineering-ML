# SDN Team Decisions Required Before Cross-Role Integration (`Phase 11.2`)

**Status:** SDN Release Candidate `SDN-RC-1.0-PHASE11.2` (`feature/sdn-controller` @ `d6bf9ad` + Phase 8.1–11 live-verified patch) is **frozen & standalone-verified** (`115/115` tests PASS on Windows/Ubuntu, `PASS_LIVE_OVS` across S0, Recovery, Dynamic Routing, and 3-cycle Stability).  
**Purpose:** Concise decision sheet of the **8 team-level decisions** required before SDN implements its northbound/data adapters and the team launches closed-loop `TC-I01`.

---

## 1. Decisions Requiring Team Approval

| ID | Decision Topic | Recommended Option | Rationale | Owner(s) | Consequence if Unlocked | Files to Update After Agreement |
| :-: | :--- | :--- | :--- | :--- | :--- | :--- |
| **D1** | `topology_version` string | Lock **`"1.0"`** everywhere (drop `"v1.0"`) | `topology_bundle.json` uses `"1.0"`; `runner_manifest.json` (`origin/main`) and `config.yaml` use `"v1.0"`. | Network + Runner | `DecisionHandler` rejects every `ApplyRequest` with `TOPOLOGY_MISMATCH`. | `configs/experiments/runner_manifest.json`, `configs/network/config.yaml` |
| **D2** | `topology_hash` & `config_hash` in `RunManifest` | Add exact SHA-256 `topology_hash` (`2fa43f87...`) from `topology_bundle.json` and real SHA-256 `config_hash` (replace `"a1b2c3d4_net_config"`) | Required by `AC-D07`, `AC-I02`, and ML `DATA_CONTRACT.md` Section C/D. | Runner + Network + Data | Fails provenance gate (`AC-I02`); Runner cannot pass verified hashes to SDN/ML. | `configs/experiments/runner_manifest.json`, `configs/network/config.yaml`, `integration/` |
| **D3** | `warmup_seconds` alignment | Lock **`60`** everywhere (matches v1.4 & `origin/main` `runner_manifest.json`) | `configs/network/config.yaml` and unmerged branches still carry `70`. | Network + Runner | Misaligned `measurement_started_at_utc` origin (`AC-I03`) and wrong snapshot cutoffs. | `configs/network/config.yaml` + rebase `feature/sdn-controller` on `main` |
| **D4** | `released_demand` transport (`REQ-03` / `MUST-LOCK #1`) | **HTTP POST** `sender.py --release-url` -> SDN Northbound API (SDN stamps `available_at_utc = now_utc`, returns `200 ACK < 5 ms`; `sender.py` adds 1 bounded retry) + `releases_<host>.jsonl` durable fallback | `sender.py` already supports `--release-url` & JSONL, but omits `available_at_utc` and has fire-and-forget `0.2s` POST with no retry. | Network + SDN + Data | Dropped HTTP releases cause `DATA_MISSING` bins in `Snapshot` and skipped control ticks. | `network/traffic_generator/sender.py` (retry), `sdn/adapters/http_control_api.py` (new) |
| **D5** | Inter-process transport & API (`IF05`/`IF08`/`IF09`/`IF10` / `MUST-LOCK #2`) | **HTTP/JSON over loopback (`127.0.0.1`)** via eventlet WSGI inside `RyuS0App` (`GET /health`, `GET /routing-state`, `POST /demand-releases`, `POST /snapshots`, `POST /apply`) + async push to Data | Proven non-blocking inside OS-Ken/eventlet during Phases 10.1 & 11 (`0` OpenFlow stalls, `1` thread). | All Roles (Runner + Data + Opt + SDN) | SDN cannot implement `http_control_api.py` or `data_event_publisher.py` without locked routes/envelope. | `sdn/adapters/http_control_api.py`, `sdn/adapters/data_event_publisher.py`, `integration/`, `data/`, `optimization/` |
| **D6** | Timing & timeout budget (`T_ctl`, `forecast`, `milp`, `apply`, `apply_guard`) | Lock pilot budget: `T_ctl=1000ms`, `forecast_timeout_ms=500` (per ML doc), `milp_timeout_ms=250`, `apply_timeout_ms=1200`, `apply_guard_ms=200`, `SDN_COUNTER_VERIFY_TIMEOUT_S=1.0s`, with `apply_deadline = forecast_end - apply_guard` (requires `H* >= 3s`) | Live OVS apply measured `170–770 ms`. Because `forecast+milp+apply` (~`1.5–2.0s`) exceeds `T_ctl=1s`, `BUSY` skips overlapping ticks safely while `apply_deadline` remains valid within `H*`. | All Roles | Premature `DEADLINE_EXPIRED` rejections or unbounded verification waits. | Frozen experiment config (`configs/`), `integration/`, `sdn/` env defaults |
| **D7** | Durable `BatchIdempotencyStore` (`MUST-LOCK #3`) & `Snapshot.content_hash` serializer | (A) Approve **local SQLite WAL** (`sdn_idempotency.sqlite3`) scoped strictly to SDN restart replay cache (Data remains sole DB writer for experiment tables).<br>(B) Freeze canonical JSON hash: `sort_keys=True, separators=(",", ":"), allow_nan=False`, ASCII-only metadata. | Guarantees crash-safe `AC-S08` replay without blocking on Data DB; eliminates `ensure_ascii` / `4` vs `4.0` hash mismatch between SDN, Data, and ML. | SDN + Data + ML | Controller restart loses in-memory idempotency cache; `Snapshot.content_hash` verification could fail across SDN/Data/ML. | `sdn/adapters/sqlite_idempotency_store.py` (SDN can build independently), `data/`, `ml/common.py` |
| **D8** | ARP policy & zero-traffic probe (`AC-S03`) | (A) Either populate static ARP (`h.setARP(...)`) in `network/topology/topology.py` **OR** lock `SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD=1` as official v1.4 ARP mode.<br>(B) Confirm whether zero-traffic flows skip switching or use active ICMP/UDP probe during `TC-I01`. | `config.yaml` says `static_entry_no_flood`, but `topology.py` has no static ARP; live OVS counter verification requires `>= 1` packet unless probe is injected. | Network + SDN + Testing | `pingall` fails without static ARP or helper; silent flows with `0` packets fail live counter verification. | `network/topology/topology.py` or `configs/network/config.yaml`, `sdn/apps/ryu_s0_app.py` |

---

## 2. SDN Adapter Split: Independent vs. Blocked on Team Approval

| SDN Adapter | Can Implement Independently Now? | Blocked On |
| :--- | :---: | :--- |
| **1. Durable Idempotency Adapter (`sdn/adapters/sqlite_idempotency_store.py`)** | **YES** (implements existing internal `BatchIdempotencyStoreProtocol`; unit-testable in pure Python) | Team sign-off on **D7(A)** before enabling in production |
| **2. Northbound Control Adapter (`sdn/adapters/http_control_api.py`)** | **NO** (wait for lock) | **D4**, **D5**, **D6** |
| **3. Data Event Publisher (`sdn/adapters/data_event_publisher.py`)** | **NO** (wait for lock) | **D5**, Data ingestion API (`data/` not yet pushed to `origin`) |

---

## 3. Minimum Entry Criteria to Start `TC-I01` (Closed-Loop Live Test)
1. Team approves **D1–D8** above and aligns `runner_manifest.json` / `config.yaml`.
2. `data/`, `optimization/`, `integration/` implementations are pushed/merged alongside `origin/feat-ml` and `feature/sdn-controller`.
3. SDN implements and unit-tests the 3 minimal adapters above (~300–400 LOC, zero changes to SDN core).
4. Live end-to-end trace runs on Ubuntu (`Network -> SDN -> Data -> ML -> Data -> Optimization -> SDN -> OVS -> Data`) with raw provenance logs collected.
