# SDN demand forecasting

- Target: requested_mbps; shared RF for 48 flows; L=30, H=1.
- Epsilon=0.0; MA W=5; baseline epsilon=0.0.
- Features: past demand lags/stats/trend and canonical flow one-hot only.
- Split: seeds/families before windowing; 50/25/25 (10/5/5 per 20 seeds). No test tuning or refit.
- Fitted 50000 / 204480 train windows; seed=2026.
- Data source: offline_network_schedule; see metadata.json for provenance.
- See validation.json for comparisons; RF superiority is not assumed.
- Lower forecasting MAE does not establish better network performance.
- Pilot latency/fallback/transport policies still need integration approval.
