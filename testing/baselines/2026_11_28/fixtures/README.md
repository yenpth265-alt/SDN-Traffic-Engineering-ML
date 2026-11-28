# Fixture bàn giao Testing
**Source checkout:** 29b95149e728078fd8f1840ffd021afb37536c7f. All 20 files in [source_manifest.json](source_manifest.json) were copied byte-for-byte from the pinned Git tree; SHA-1 Git blob and SHA-256 independently computed and checked. Capture provenance distinguishes producer execution SHA `a64916e` from current repo source pin/capture commit `fa53213`. Fixtures are controlled/synthetic data, not live production or OVS proof.

| Directory | Payload and provenance | Usage |
|---|---|---|
| online_contract/ | Actual Network telemetry producer dry-run (24 links); SDN snapshot_creator L2/history96, nested sdn + legacy compact test-only; Predictor actual 240 Prediction rows H5; Optimization/SDN Apply capture, result and result_full, expiry; capture_provenance with source hashes | TC-D04/I02/I03/I04; HTTP snapshot body wrap ONLY sdn; forecast array is rows, use test wrapper; apply result_full=48; single result=negative |
| ml_data_v14/ | Native frozen model RF batch 48 H1 rows, L30 synthetic Snapshot 1440, context and verification | TC-M01; frozen ML output, not DB/live actual; use only matching run/context |
| sdn/data_v14_v10/ | Canonical topology v1.0 L10/L30 valid, incomplete and stale generated from controlled history; original README and verifier script | TC-I05; stale is valid schema but rejected for decisions |
| optimization/wire/ | Native wire DecisionBatch 48 and changed-only ApplyRequest, zero B/no-op | TC-O03, contract check schema; fixed synthetic identifiers |
| negative/ | QA-derived malformed cases, original source untouched; [negative_manifest.json](negative_manifest.json) records origin/mutation/expected/status | Isolated validator only; NOT_RUN as newly packaged payload variants |

**Important:** Never directly send archived old timestamps, fixture run IDs, fake paths or placeholder config hash to a live controller. Recreate registration, topology/demand/routing references and timestamps through existing test suite/producer; do not edit captured files to claim PASS. Schema-only checks vs real PostgreSQL checks distinct. In particular snapshot wire hash must be computed from exact body bytes; `online_contract/snapshot.json` is a capture wrapper, `data_v1` compact test helper must not be posted; SDN canonical standalone body has raw transport digest. Fixtures are not a unified single run; provenance and context are independent.

Reproduce from the repository root checkout 29b95149e728078fd8f1840ffd021afb37536c7f:
- `python -m testing.scripts.capture_contract_fixtures testing/fixtures/online_contract`
- `python -m testing.scripts.generate_ml_data_fixture`
- `python sdn/fixtures/data_v14_v10/verify_v10_fixtures.py --verify`
- Execute G1 workflow with disposable PostgreSQL and same checkout. Source README paths in preserved fixture dirs are relative to original repo, not this documentation bundle.
- Demand, traffic, run registration, late arrival, version, deadline test inputs are built dynamically by `testing/scripts/test_postgresql_contracts.py`, `data/ingestion/test_online_ingestion.py` and `sdn/tests/integration_postgres_v14.py` so actual UTC and FK references are consistent; do not fabricate static live demand events.

