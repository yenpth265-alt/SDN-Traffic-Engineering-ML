# Canonical Network `v1.0` SDN Snapshot v1.4 Fixtures (`CF-05` + `CF-21`)

- **Canonical Network Source:** `origin/main` @ `a2d97687ced118879e0908516beb50f1feb3992d`
- **Approved Topology Identity:**
  - `topology_version`: `"v1.0"`
  - `topology_hash`: `ec6c6969019b1c36744339fb54f0f608c95402ae972859b30710dc44b5dc3956` (raw 64-char lowercase hex)
  - `ovs_port_mapping_verified`: `false` (Colab export; live OVS evidence is tracked separately at SHA `2ec0a583d6482d1ebcb749167d4fafeb22a8ec5e`)
- **Historical Fixtures Preserved:** Original `1.0` (`2fa43f87...`) fixtures in `sdn/fixtures/data_v14/` are kept intact and untouched.

## Fixture Inventory
1. `snapshot_valid_l10_body.json` — `VALID`, `L=10`, `480/480` valid demand observations, `48/48` `VERIFIED` routes.
2. `snapshot_valid_l30_body.json` — `VALID`, `L=30`, `1440/1440` genuine observations (no padding, no fake zeros), `48/48` `VERIFIED` routes (ML `L=30` / CF-21).
3. `snapshot_incomplete_body.json` — `INCOMPLETE_HISTORY`, `L=10`, `479/480` valid, 1 missing observation preserved with `requested_mbps=null`, `valid=false`, `released_at_utc=null`, `available_at_utc=null`, `reason=null`.
4. `snapshot_stale_body.json` — `STALE`, `L=10`, `reason=DATA_STALE` (`as_of - cutoff = 2.5s > T_stale = 2.0s`).

## Verification Command
```bash
python sdn/fixtures/data_v14_v10/verify_v10_fixtures.py --verify
```
