# Canonical SDN v1.4 Data API Request Fixtures (`sdn/fixtures/data_v14/`)

- **SDN Release Commit:** `ecc63e56aacf4894aa7a24dbfe6840d28bb66e69` (`feature/sdn-data-contract-v14`)
- **Data Reference Baseline:** `origin/main` @ `8ee1782554f48296942595873aaa989e9b4ca637`
- **Data Source Label:** **SYNTHETIC DETERMINISTIC FIXTURES** (`run_id = synthetic_sdn_v14_fixture_run_01`) generated strictly by calling real SDN production adapters (`map_snapshot_to_data_v1`, `map_routing_state_to_data_v1`, `build_48_flow_apply_result_data_v1`, `build_noop_48_flow_apply_result_data_v1`, `build_data_v1_headers`). Never hand-edited.

## Fixture Catalog & Verified Hashes

| File | Endpoint | Bytes | `X-Content-SHA256` (exact UTF-8 bytes) | `sdn.content_hash` |
|---|---|---:|---|---|
| `snapshot_valid_body.json` | `POST /v1/sdn/snapshots` | 189,207 | `a48f7093ed630840620699cdab72659dc1429c765a9271a05bbf126b0f7af0c7` | `9b78917b787b9dd776b4da0b20c19850103f540b0c9676cfc114b6664438291c` |
| `snapshot_valid_headers.json` | Headers for `snapshot_valid_body.json` | 266 | `5b80b081a5d9cb8b61914da0f3140810775fd916e80bb0a3952969bcd02edf43` | *(n/a)* |
| `snapshot_incomplete_body.json` | `POST /v1/sdn/snapshots` (`INCOMPLETE`) | 189,140 | `9333858ee99d084e414b321d668274a260ef86d64b392e9855864adf685d9987` | `e8a9827457e471ebd078e02f240d83e7a264f019ecaf5a276117d08510150cc2` |
| `routing_state_body.json` | `POST /v1/sdn/routing` | 5,970 | `80f22b2d9f99b39c3c83c2b45e510f28b858dd0e2e05ab5b38e5a80181f60dd2` | *(n/a)* |
| `apply_result_1_applied_47_kept.json` | `POST /v1/sdn/apply-results` (`APPLIED`) | 6,203 | `292cd17a4c2e6fb268663e64387a8443a748a9b0125d7f65003d5e915e4aa072` | *(n/a)* |
| `apply_result_rollback.json` | `POST /v1/sdn/apply-results` (`ROLLED_BACK`) | 6,223 | `aabcf926499bd4f5a567b3ad0f98705657696b7c23089962d700a17ef9a4b646` | *(n/a)* |
| `apply_result_unknown.json` | `POST /v1/sdn/apply-results` (`INCONSISTENT`) | 6,197 | `7debe9840e56f5210cd396b9083cad16913b81c71863ae66eeb06b98af974874` | *(n/a)* |
| `apply_result_noop.json` | `POST /v1/sdn/apply-results` (`B=0 KEPT`) | 6,197 | `3c9332747a16c51c9c983e42bf1bc71ee8ab9a566855e50847e5c3b570cb7ad3` | *(n/a)* |

## Snapshot Parameters
- 48 canonical flow groups (`0x103 .. 0x806`), `L = 10` (`480` history bins), `cutoff_offset_s = 100`, `warmup_s = 60`, `delta_s = 1`.
- Measurement bins `90..99` (`09:02:30Z .. 09:02:40Z`), measurement cutoff `100` corresponds to traffic bin `160` (`09:00:00Z + 160s`).
- `snapshot_incomplete_body.json` omits `(0x103, bin 99)` and explicitly emits `valid=false, requested_mbps=null, released_at_utc=null, available_at_utc=null, release_record_id=null, reason=null`.

## Reproducible Verification Command
```bash
python sdn/fixtures/data_v14/verify_contract_fixtures.py --verify
```
To regenerate deterministically:
```bash
python sdn/fixtures/data_v14/verify_contract_fixtures.py --generate --verify
```

## QA PostgreSQL CI Handoff
In `.github/workflows/postgresql_interface_contracts.yml` (on QA branch):
1. Pin `producer_src/sdn` to `ecc63e56aacf4894aa7a24dbfe6840d28bb66e69`.
2. Apply `schema_postgresql_dbeaver.sql`, `002_online_routing.sql`, `003_forecast_measurement_origin.sql`, and `004_decision_tick_order.sql`.
3. Run `python -m unittest -v data.ingestion.test_management_api data.ingestion.test_online_ingestion testing.scripts.test_postgresql_contracts sdn.tests.integration_postgres_v14` and assert **0 skipped** integration tests.
