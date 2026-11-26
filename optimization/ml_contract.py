"""Use the same frozen ML selection and consume final forecasts without calibration."""
from datetime import timedelta
from collections.abc import Mapping
from ml.common import require, utc, stamp
from ml.deployment import read_deployment


def normalize_forecast(forecast, groups, deployment_file):
    selected = read_deployment(deployment_file)
    H = selected["horizon_H"]
    result = dict(forecast)
    if "rows" in forecast:
        rows = forecast["rows"]
        require(len(rows) == len(groups) * H, "Forecast rows differ from frozen 48 x H")
        vectors = {fg: {} for fg in groups}
        first = rows[0]
        for row in rows:
            fg, start = row["flow_group_id"], row["forecast_start"]
            require(fg in vectors and type(start) is int and type(row["forecast_end"]) is int
                    and row["forecast_end"] == start + 1 and start not in vectors[fg],
                    "Invalid or duplicate forecast flow/bin")
            require(row["run_id"] == forecast["run_id"] and row["issued_at"] == first["issued_at"]
                    and row["source"] == first["source"], "Mixed forecast metadata")
            vectors[fg][start] = row["predicted_mbps"]
        anchor = min(vectors[groups[0]])
        expected = list(range(anchor, anchor + H))
        require(all(sorted(values) == expected for values in vectors.values()), "Incomplete forecast horizon")
        origin = utc(forecast["measurement_started_at_utc"])
        result["predictions"] = {fg: {"predicted_mbps_array": [vectors[fg][i] for i in expected]}
                                 for fg in groups}
        result["forecast_start"] = stamp(origin + timedelta(seconds=anchor))
        result["forecast_end"] = stamp(origin + timedelta(seconds=anchor + H))
    predictions = result.get("predictions", result)
    require(all(fg in predictions and len(predictions[fg].get("predicted_mbps_array", [])
                                        if isinstance(predictions[fg], Mapping) else predictions[fg]) == H
                for fg in groups),
            "Forecast H differs from frozen ML YAML")
    if result.get("forecast_start") and result.get("forecast_end"):
        require((utc(result["forecast_end"]) - utc(result["forecast_start"])).total_seconds() == H,
                "Forecast time window differs from frozen H")
    return result
