# Hợp đồng giao diện tích hợp Testing
**Baseline:** `main@29b95149e728078fd8f1840ffd021afb37536c7f` — 28/11/2026. **Trạng thái:** bản đồng bộ QA để owner rà soát; chưa sign-off toàn hệ thống. Nguồn phiên bản: [baseline_manifest.json](baseline_manifest.json).

## 1. Quy ước chung
Data API v1, SDN Snapshot v1.4 nested, topology v1.0. Catalog: `0x103` (hex src×256+dst), path `0x103_P1`/`_P2`, link `e1>c1`; không alias h1_h3. 48 flows, 96 paths, 32 directed links, 16 fabric, 24 measurable links; topology_hash raw 64 lowercase hex. Network network_config_hash, per-run config_hash (propagate nguyên vẹn) và optimizer_config_hash là ba namespace; không ghi đè nhau. Model hash/ML config hash độc lập.

UTC aware; traffic bin B_t gồm warm-up, measurement bin B_m không gồm warm-up. Δ=1 s, B_t=B_m+60; cutoff=100 → traffic 160, 100 ở measurement. History bins [cutoff−L,cutoff), forecast rows [cutoff+i,cutoff+i+1), H1→48 rows; H5 regression→240. Không bin âm/float. available_at_utc do DB cấp; valid history phải released/available ≤ as_of, không nằm tương lai so với cutoff. Mẫu thiếu giữ null/reason; cấm padding và demand_plan cho online M1–M3; chỉ oracle M4.

## 2. Ma trận wire/API

| IF | Producer → consumer / role | Endpoint | Schema & entry point/validator | Evidence / case |
|---|---|---|---|---|
| IF-01 | Launcher/Runner → Data (RUNNER) | POST /v1/runs; GET /v1/runs/{run_id}; PATCH /v1/runs/{run_id}/status | context.manifest_to_run → management_api.RunCreate/RunStatus; register validates origins/ML context and idempotency | TC-R01/R02; SQL run row=1 |
| IF-02 | Network catalog → Data/Runner/SDN/ML/G0 | GET /v1/runs/{run_id}/topology | exporter.py, TopologyRegistry.from_bundle_file, Data topology; manifest binding | TC-N01/R01 |
| IF-03 | Network sender → Data (NETWORK) | POST /v1/network/demand | online_models.Demand, online_api.demand → online_store ingest; one flow/bin, stable event_id, window and release | TC-D01/D02/I02 |
| IF-04 | Network correlate → Data (NETWORK) | POST /v1/network/traffic | TrafficBatch + Traffic, online_api.traffic → store; sent/recv counts/bytes nullable, FK all-or-nothing | TC-D03/E01 |
| IF-05 | Network collector → Data (NETWORK/SDN) | POST /v1/sdn/links | LinkBatch/Link, check_batch() & DB catalog link/port coverage; 24 measured links | TC-D04/N03; telemetry capture |
| IF-06 | Data → SDN (SDN) | GET /v1/runs/{run_id}/demand-history?cutoff=&as_of=&lookback= | online_api.history → store, DataAPIDemandHistoryProvider | TC-I02/I03 |
| IF-07 | SDN → Data (SDN) | POST /v1/sdn/routing | Routing/Route, online_api.routing & store; 48 rows, version, verification status | TC-S01/I04 |
| IF-08 | SDN → Data → ML (SDN writer, SDN/READER reader) | POST /v1/sdn/snapshots; GET /v1/runs/{run_id}/snapshots/{snapshot_id} | Snapshot.validate_contract, SDNSnapshotV14/Snapshot; raw header hash + stored bytes/evidence | TC-D05/I02/I05 |
| IF-09 | ML → Data (ML) | POST /v1/ml/forecasts | OnlineForecastRunner, ForecastBatch + Prediction, ingest_forecast; strict integer windows, READY complete | TC-M01/M02/I03 |
| IF-10 | Data → Optimization (OPTIMIZATION) via Runner | GET /v1/runs/{run_id}/forecasts/{forecast_batch_id} | ready_forecast_context; validate_context_binding, validate_forecast, validate_routing_state | TC-O01/O02 |
| IF-11 | Optimization → Data (OPTIMIZATION) | POST /v1/optimization/decisions; GET /v1/runs/{run_id}/decisions/{decision_batch_id} | result_serializer.build_decision_wire_payload, Decision/DecisionRow, management_api; 48 rows; correct tick/order/version/deadline | TC-O03/I04 |
| IF-12 | Runner/Optimization → SDN (X-SDN-Token) | GET /health, GET /routing-state?run_id=…, POST /snapshots, POST /apply, GET /apply-status?run_id=…&decision_batch_id=…, POST /reconcile-audit | ApplyRequest.validate_contract, DecisionHandler, HTTP control adapter; B changed flows only, polling IDs, reconcile protected | TC-S01/S02/S03 |
| IF-13 | SDN → Data (SDN) | POST /v1/sdn/apply-results | ApplyResult/ResultRow, management_api/results; 48 per-group results, routing published first | TC-I04/I05 |
| IF-14 | Modules → Data (component role) | POST /v1/runs/{run_id}/events/{component} | Event, durable audit; RoutingHistory/VerificationEvidence in detail, Data idempotency | TC-R02/D06 |
| IF-15 | Runner → QA raw store | manifest + raw/ logs, execution.json | run_benchmarks.py, validate_results.recompute/paired_differences; checksums/cases quality | TC-E01/E02/E03 |

Endpoint ownership: Data validates schema/control invariants, stores and serves; SDN creates Snapshot and physical verification; ML creates raw/final forecast, G0 computes mean and optimization; Data never fills missing history or recomputes mean. SDN Northbound route names are not Data routes; legacy /v1/... for SDN apply is not current.

## 3. Field contracts (wire)

- **RunCreate:** run_id, topology_id, schedule_id, mode (calibration/benchmark/live/oracle), method_id, run_started_at_utc, traffic_started_at_utc, measurement_started_at_utc, warmup_s, measurement_s, schema_version, config_version, config_hash, ml_config_hash/lookback_bins/horizon_bins/model_version/calibration_version when applicable, git_commit, manifest. RunStatus terminal ok/failed/discarded; identical replay accepted, terminal not reopened.
- **Demand:** run_id,event_id,flow_group_id,bin_index,window_start_utc,window_end_utc,source=network,requested_mbps,released_at_utc,reason. Data adds availability; zero is real zero, null lacks observation. **TrafficBatch:** run_id, rows[flow_group_id,window_start/end_utc,sent_bytes,received_bytes,sent_packets,received_packets].
- **LinkBatch:** run_id,batch_id,sampled_at_utc,bin_index,interval_s,sample_count,samples; each link_id,measure_switch,measure_port,tx_bytes_total,delta_bytes,delta_t_s,port_tx_mbps,counter_status, optional from_node,to_node,is_fabric,utilization,tx_drops_delta. No duplicate link; 24 measurable catalog links, missing/empty invalid; use one directed egress counter, invalid delta null.
- **Routing:** run_id,state_version,published_at_utc,rows[flow_group_id,path_id,confirmed_at_utc,verification_status VERIFIED/UNKNOWN/INCONSISTENT]; 48 flow identities canonical.
- **Snapshot:** exact body {"sdn":{...}} with snapshot_id,run_id,snapshot_as_of_utc,history_cutoff_utc,cutoff_offset_s,lookback_L,delta_s,state_version,config_version/hash,topology_version/hash,content_hash,origin_refs,flow_group_ids,history,routing_state,routing_history,link_sample_refs,quality_status,missing_groups,routing_state_complete,unknown_flow_group_ids,provenance,reason,expected_bin_count,valid_bin_count. History row flow_group_id,bin_index,window_start/end_utc,requested_mbps,released_at_utc,available_at_utc,valid,reason,release_record_id. Quality VALID only 48×L causal observations and 48 verified routes; other INCOMPLETE/STALE/INVALID not usable. Body hash X-Content-SHA256 = exact bytes; content_hash = SHA-256 canonical compact sorted JSON of sdn excluding content_hash; GET identical bytes/hash. No compact data_v1 object on wire.
- **ForecastBatch:** run_id,forecast_batch_id,snapshot_id,rows. Exactly seven fields per Prediction: run_id,flow_group_id,issued_at,forecast_start,forecast_end,predicted_mbps,source. Single issue/run/snapshot, complete 48×H rows, strict integer bins, finite nonnegative Mbps; no extra model fields in rows. Run context frozen H, Data stores atomically/READY; no mean. Runner binds READY envelope run/snapshot/routing origins, hashes, horizon/deadline. G0 computes mean of final ML values; epsilon already applied in ML (0 on deployed artifact).
- **DecisionBatch:** run_id,decision_batch_id,tick_id,tick_at_utc,snapshot_id,forecast_batch_id,expected_state_version,apply_deadline_utc,optimizer_result,rows[flow_group_id,old_path_id,proposed_path_id]. 48 rows; B changed max. POST to Data before SDN /apply. No-op B0 resolves without fabricated ApplyResult when Data marks tick complete.
- **ApplyRequest:** strict `sdn/schemas/apply_models.py` core and transport envelope; use emitted wire fixture, do not send rich optimizer audit; bind actual decision ID and recalculate payload hash. Only changed flows; apply deadline ≤ forecast_end, version expected, reject expired prewrite, no concurrent apply while pending. HTTP 202 poll_url not terminal; preserve run/batch ID.
- **ApplyResult:** run_id,decision_batch_id,response_status (APPLIED/REJECTED/FAILED/ROLLED_BACK/PARTIAL/INCONSISTENT), response_state_version,completed_at_utc,rows[flow_group_id,actual_path_id,status APPLIED/KEPT/FAILED/ROLLED_BACK/UNKNOWN,verified_at_utc,reason]. 48 unique/consistent rows; APPLIED path verified proposed; KEPT/ROLLED_BACK old verified; UNKNOWN path/time null. Publish verified routing before final result, version +1 when verified state changes (maximum once per batch), no version increase for no-op; reconcile if unknown. RoutingHistory/VerificationEvidence through events with full detail. No APPLIED from FlowMod/Barrier alone.

## 4. ACK, transport và lifecycle

| Response | Producer/QA action |
|---|---|
| HTTP 200 inserted>0 / updated>0 | ACK new write; verify expected cardinality/ID/version from GET hoặc SQL |
| HTTP 200 inserted:0 / updated:0 | replay identical, no additional rows; compare key/payload + read-back |
| 202 SDN apply | pending job; poll proper run/batch until terminal or reconcile/expiry; not delivery PASS |
| 401/403 | role/key unauthorized; fail preflight, no data accepted |
| 409 | ID content/state conflict or non-VALID snapshot, fail/diagnose, no blind retry |
| 422 | schema/invariant invalid; reject, verify no partial insert |
| network timeout/5xx | bounded retry exact bytes/ID via durable outbox, reconcile before next; no overwrite or retry after unsafe expiry |

ACK content matters: no success from exit code/stdout; review outbox state + SQL by run/bin/flow/batch. At terminal drain no pending journals; otherwise failed + retained evidence. Data keys per role via env, never commit/log; SDN Northbound secret separate.

Runner must use explicit integration/offline/dry-run mode. Integration requires `--release-url http(s)://<DATA_HOST>:<PORT>/v1/network/demand`, `--telemetry-url http(s)://<DATA_HOST>:<PORT>/v1/sdn/links`, role NETWORK `DATA_API_KEY` in env; fail before launch if either missing, invalid URL or key. Offline/dry-run no URLs; dry-run no process launch. External preflight package implements this locally; integration into native launcher and ACK proof pending. Correlate traffic during drain, confirm outboxes empty. Profile entry points and schema at [runtime runbook](https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML/blob/29b95149e728078fd8f1840ffd021afb37536c7f/integration/orchestration/README.md).

## 5. Sources/validation
`data/ingestion/online_models.py`, `online_api.py`, `online_store.py`, `management_api.py`; `sdn/schemas/{snapshot_models,apply_models}.py`; `ml/inference/online.py`; `optimization/input_validator.py`, `result_serializer.py`, `service_client.py`; `integration/orchestration/{context,runner}.py`. Exact blob IDs in manifest, fixture registry in fixtures. Runtime contract current over draft v2 and historical examples. Full live verification remains required.

