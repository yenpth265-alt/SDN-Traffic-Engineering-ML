"""Online Data API client and tick runner for ML forecasting (v1.4)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request

from ml.common import ContractError, ROW_FIELDS, numbers, require, stamp, utc, whole
from ml.inference.predict import Predictor, unwrap_snapshot
from ml.inference.replay import ReplayJournal

METHOD_BY_ID = {
    "M1": "persistence",
    "M2": "moving_average",
    "M3": "rf",
}
RETRYABLE_HTTP_STATUS = frozenset({500, 502, 503, 504})


class DataApiError(RuntimeError):
    """HTTP or transport failure returned by the Data API."""

    def __init__(self, status_code: int | None, detail: str, method: str, url: str):
        self.status_code = status_code
        self.detail = detail
        self.method = method
        self.url = url
        code = f"HTTP {status_code}" if status_code is not None else "NETWORK"
        super().__init__(f"{method} {url} failed ({code}): {detail}")


def _encode_json(payload) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def resolve_method(method: str, run_info: dict | None = None) -> str:
    """Resolve 'auto' from RunCreate.method_id or validate an explicit method name."""
    if method == "auto":
        require(run_info is not None and run_info.get("method_id") in METHOD_BY_ID,
                "Cannot resolve method='auto' without run method_id in {M1, M2, M3}")
        return METHOD_BY_ID[run_info["method_id"]]
    require(method in ("rf", "persistence", "moving_average"), f"Unknown method: {method}")
    if run_info is not None:
        require(METHOD_BY_ID.get(run_info.get("method_id")) == method,
                "Explicit method differs from registered run method_id")
    return method


def build_forecast_batch(
    run_id: str,
    snapshot_id: str,
    rows: list[dict],
    forecast_batch_id: str | None = None,
    method: str = "rf",
    snapshot: dict | None = None,
    horizon_H: int | None = None,
) -> dict:
    """Wrap 48 x H seven-field Prediction rows into Data API's ForecastBatch payload."""
    require(bool(rows), "Cannot build ForecastBatch from empty rows")
    groups = {hex(s * 256 + d) for s in range(1, 9) for d in range(1, 9)
              if (s - 1) // 2 != (d - 1) // 2}
    H = horizon_H if horizon_H is not None else len(rows) // 48
    require(type(H) is int and H > 0 and len(rows) == 48 * H, "Forecast must contain exactly 48 x H rows")
    require(all(set(row) == set(ROW_FIELDS) for row in rows), "Prediction rows must have exactly seven fields")
    require(all(type(row["forecast_start"]) is int and type(row["forecast_end"]) is int for row in rows),
            "Forecast offsets must be strict integers")
    anchor = min(row["forecast_start"] for row in rows)
    require(anchor >= 0, "Negative forecast offset")
    expected = {(fg, offset) for fg in groups for offset in range(anchor, anchor + H)}
    seen = set()
    for row in rows:
        key = (row["flow_group_id"], row["forecast_start"])
        require(key not in seen and key in expected and row["run_id"] == run_id
                and row["forecast_end"] == row["forecast_start"] + 1
                and row["issued_at"] == rows[0]["issued_at"] and row["source"] == rows[0]["source"],
                "Incomplete, duplicate or inconsistent forecast rows")
        seen.add(key)
    numbers([row["predicted_mbps"] for row in rows], "predicted_mbps")
    if snapshot is not None:
        inner = unwrap_snapshot(snapshot)
        require(inner["run_id"] == run_id and inner["snapshot_id"] == snapshot_id,
                "Forecast snapshot/run mismatch")
        require(inner["cutoff_offset_s"] == anchor, "Forecast bins differ from snapshot cutoff")
    batch_id = forecast_batch_id or f"{run_id}:{snapshot_id}:{method}"
    return {
        "run_id": run_id,
        "forecast_batch_id": batch_id,
        "snapshot_id": snapshot_id,
        "rows": rows,
    }


def build_context(
    run_info: dict,
    snapshot_payload: dict,
    predictor: Predictor,
    *,
    max_snapshot_age_s: float = 2.0,
    forecast_timeout_ms: float = 500.0,
    fallback_policy: str = "skip",
    fallback_epsilon: float = 0.0,
) -> dict:
    """Construct Predictor run_context from GET /v1/runs/{run_id} and Snapshot v1.4."""
    inner = unwrap_snapshot(snapshot_payload)
    run_id = run_info["run_id"]
    require(inner.get("run_id") == run_id, "Snapshot run_id differs from registered run")
    if run_info.get("config_hash"):
        require(inner.get("config_hash") == run_info["config_hash"],
                "Snapshot config_hash differs from registered run")
    if run_info.get("config_version"):
        require(inner.get("config_version") == run_info["config_version"],
                "Snapshot config_version differs from registered run")
    if run_info.get("model_version"):
        require(run_info["model_version"] == predictor.config["model_version"],
                "Run model_version differs from frozen ML config")
    if run_info.get("lookback_bins") is not None:
        require(whole(run_info["lookback_bins"], "lookback_bins", 1) == predictor.config["lookback_L"],
                "Run lookback_bins differs from frozen ML config")
    if run_info.get("horizon_bins") is not None:
        require(whole(run_info["horizon_bins"], "horizon_bins", 1) == predictor.config["horizon_H"],
                "Run horizon_bins differs from frozen ML config")
    if run_info.get("ml_config_hash"):
        require(run_info["ml_config_hash"] == predictor.metadata["ml_config_sha256"],
                "Run ml_config_hash differs from frozen ML artifact")

    origin = utc(run_info["measurement_started_at_utc"])
    measurement_s = whole(run_info["measurement_s"], "measurement_s", 1)
    measurement_end = stamp(origin + timedelta(seconds=measurement_s))

    topology_hash = run_info.get("topology_hash") or inner.get("topology_hash")
    if run_info.get("topology_id") and len(str(run_info["topology_id"])) == 64:
        require(run_info["topology_id"] == inner.get("topology_hash"),
                "Snapshot topology_hash differs from registered topology_id")

    return {
        "run_id": run_id,
        "snapshot_id": inner["snapshot_id"],
        "snapshot_content_hash": inner["content_hash"],
        "config_hash": run_info.get("config_hash") or inner["config_hash"],
        "topology_hash": topology_hash,
        "ml_config_sha256": predictor.metadata["ml_config_sha256"],
        "model_sha256": predictor.metadata["model_sha256"],
        "measurement_started_at_utc": stamp(origin),
        "measurement_end_utc": measurement_end,
        "max_snapshot_age_s": float(max_snapshot_age_s),
        "forecast_timeout_ms": float(forecast_timeout_ms),
        "fallback_policy": fallback_policy,
        "fallback_epsilon": float(fallback_epsilon),
    }


class DataApiClient:
    """Minimal HTTP client for Data API v1 endpoints used by ML."""

    def __init__(
        self,
        base_url: str,
        *,
        ml_api_key: str | None = None,
        reader_api_key: str | None = None,
        timeout_s: float = 5.0,
        max_retries: int = 2,
        retry_backoff_s: float = 0.1,
        opener=None,
    ):
        require(isinstance(base_url, str) and base_url.strip(), "base_url is required")
        self.base_url = base_url.rstrip("/")
        self.ml_api_key = ml_api_key if ml_api_key is not None else os.getenv("SDN_ML_KEY")
        self.reader_api_key = (
            reader_api_key
            if reader_api_key is not None
            else (os.getenv("SDN_READER_KEY") or self.ml_api_key)
        )
        self.timeout_s = float(timeout_s)
        self.max_retries = whole(max_retries, "max_retries")
        self.retry_backoff_s = float(retry_backoff_s)
        self._opener = opener or urllib.request.urlopen

    def _require_key(self, key: str | None, name: str) -> str:
        require(isinstance(key, str) and bool(key), f"Missing API key ({name})")
        return key

    def _request(
        self,
        method: str,
        path: str,
        *,
        api_key: str | None = None,
        body: bytes | None = None,
        retryable: bool = False,
    ) -> tuple[bytes, dict[str, str]]:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        if api_key is not None:
            headers["X-API-Key"] = api_key
        if body is not None:
            headers["Content-Type"] = "application/json"

        attempts = (self.max_retries + 1) if retryable else 1
        for attempt in range(attempts):
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with self._opener(req, timeout=self.timeout_s) as resp:
                    raw = resp.read()
                    resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                    return raw, resp_headers
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                if retryable and exc.code in RETRYABLE_HTTP_STATUS and attempt + 1 < attempts:
                    time.sleep(self.retry_backoff_s * (2 ** attempt))
                    continue
                raise DataApiError(exc.code, detail, method, url) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if retryable and attempt + 1 < attempts:
                    time.sleep(self.retry_backoff_s * (2 ** attempt))
                    continue
                raise DataApiError(None, str(exc), method, url) from exc
        raise AssertionError("Unreachable retry state")

    def health_ready(self) -> dict:
        raw, _ = self._request("GET", "/health/ready", retryable=True)
        return json.loads(raw.decode("utf-8"))

    def get_run(self, run_id: str) -> dict:
        key = self._require_key(self.ml_api_key, "SDN_ML_KEY")
        encoded_run = urllib.parse.quote(run_id, safe="")
        raw, _ = self._request("GET", f"/v1/runs/{encoded_run}", api_key=key, retryable=True)
        return json.loads(raw.decode("utf-8"))

    def get_snapshot(self, run_id: str, snapshot_id: str) -> tuple[dict, str]:
        key = self._require_key(self.reader_api_key, "SDN_READER_KEY")
        encoded_run = urllib.parse.quote(run_id, safe="")
        encoded_snap = urllib.parse.quote(snapshot_id, safe="")
        raw, headers = self._request(
            "GET",
            f"/v1/runs/{encoded_run}/snapshots/{encoded_snap}",
            api_key=key,
            retryable=True,
        )
        header_sha = headers.get("x-content-sha256", "")
        require(len(header_sha) == 64, "Missing or malformed X-Content-SHA256 header")
        actual_sha = hashlib.sha256(raw).hexdigest()
        require(
            hmac.compare_digest(actual_sha, header_sha.lower()),
            "Snapshot X-Content-SHA256 mismatch against downloaded bytes",
        )
        return json.loads(raw.decode("utf-8")), actual_sha

    def post_forecast_batch(self, batch_payload: dict) -> dict:
        key = self._require_key(self.ml_api_key, "SDN_ML_KEY")
        raw, _ = self._request(
            "POST",
            "/v1/ml/forecasts",
            api_key=key,
            body=_encode_json(batch_payload),
            retryable=True,
        )
        return json.loads(raw.decode("utf-8"))

    def post_ml_event(
        self,
        run_id: str,
        event_id: str,
        event_type: str,
        detail: dict,
        event_at_utc: str | None = None,
    ) -> dict:
        key = self._require_key(self.ml_api_key, "SDN_ML_KEY")
        encoded_run = urllib.parse.quote(run_id, safe="")
        payload = {
            "event_id": event_id,
            "event_at_utc": event_at_utc or stamp(datetime.now(timezone.utc)),
            "event_type": event_type,
            "detail": detail,
        }
        raw, _ = self._request(
            "POST",
            f"/v1/runs/{encoded_run}/events/ml",
            api_key=key,
            body=_encode_json(payload),
            retryable=True,
        )
        return json.loads(raw.decode("utf-8"))


class OnlineForecastRunner:
    """Keep one frozen Predictor loaded and execute causal online forecast ticks."""

    def __init__(self, artifact_path, client: DataApiClient, *, replay_db=None):
        self.predictor = Predictor(artifact_path)
        self.client = client
        default_db = Path(__file__).resolve().parents[1] / "artifacts/online-replay.sqlite3"
        self.journal = ReplayJournal(replay_db or os.getenv("SDN_ML_REPLAY_DB") or default_db)
        self._runs: dict[str, dict] = {}

    def get_run(self, run_id: str, *, refresh: bool = False) -> dict:
        if refresh or run_id not in self._runs:
            run = self.client.get_run(run_id)
            require(run.get("run_id") == run_id, "Downloaded run_id differs from requested run_id")
            self._runs[run_id] = run
        return self._runs[run_id]

    def process_snapshot(
        self,
        run_id: str,
        snapshot_id: str,
        *,
        forecast_batch_id: str | None = None,
        method: str = "auto",
        max_snapshot_age_s: float = 2.0,
        forecast_timeout_ms: float = 500.0,
        fallback_policy: str = "skip",
        fallback_epsilon: float = 0.0,
        emit_event: bool = False,
        dry_run: bool = False,
        clock=None,
    ) -> dict:
        """Freeze each forecast before POST; a repeated tick reuses its payload."""
        run_info = self.get_run(run_id)
        self.predictor.last_audit = {"status": "INVALID", "snapshot_id": snapshot_id, "fallback": False}
        resolved_method = resolve_method(method, run_info)
        batch_id = forecast_batch_id or f"{run_id}:{snapshot_id}:{resolved_method}"
        snapshot_payload, raw_sha256 = self.client.get_snapshot(run_id, snapshot_id)
        audit = None
        try:
            require(unwrap_snapshot(snapshot_payload).get("snapshot_id") == snapshot_id,
                    "Downloaded snapshot_id differs from requested snapshot_id")
            context = build_context(
                run_info,
                snapshot_payload,
                self.predictor,
                max_snapshot_age_s=max_snapshot_age_s,
                forecast_timeout_ms=forecast_timeout_ms,
                fallback_policy=fallback_policy,
                fallback_epsilon=fallback_epsilon,
            )
            binding = {
                "snapshot_id": snapshot_id,
                "snapshot_http_sha256": raw_sha256,
                "method": resolved_method,
                "context": {key: context[key] for key in (
                    "run_id", "config_hash", "topology_hash", "ml_config_sha256",
                    "model_sha256", "measurement_started_at_utc", "measurement_end_utc",
                )},
            }

            def build():
                rows = self.predictor.predict_batch(
                    snapshot_payload, context, method=resolved_method, clock=clock
                )
                batch = build_forecast_batch(
                    run_id, snapshot_id, rows, forecast_batch_id=batch_id,
                    method=resolved_method, snapshot=snapshot_payload,
                    horizon_H=self.predictor.config["horizon_H"],
                )
                audit = dict(self.predictor.last_audit)
                audit.update(snapshot_http_sha256=raw_sha256, forecast_batch_id=batch_id)
                return batch, audit

            if dry_run:
                batch_payload, audit = build()
                replayed = False
            else:
                batch_payload, audit, replayed = self.journal.freeze(
                    self.client.base_url, "forecast", run_id, batch_id, binding, build,
                )
            cutoff = utc(unwrap_snapshot(snapshot_payload)["history_cutoff_utc"])
            delivery_now = clock() if clock else datetime.now(timezone.utc)
            require(delivery_now < cutoff + timedelta(seconds=self.predictor.config["horizon_H"]),
                    "FORECAST_EXPIRED_REPLAY" if replayed else "FORECAST_EXPIRED")
            require((delivery_now - cutoff).total_seconds() <= float(max_snapshot_age_s),
                    "STALE_HISTORY_REPLAY" if replayed else "STALE_HISTORY_AT_DELIVERY")
        except ContractError as exc:
            audit = dict(audit if audit is not None else self.predictor.last_audit)
            audit.update(status="INVALID", reason=str(exc), snapshot_id=snapshot_id)
            audit["snapshot_http_sha256"] = raw_sha256
            if emit_event and not dry_run:
                audit["event_delivery"] = self._try_emit_event(
                    run_id,
                    f"ml:{run_id}:{snapshot_id}:{resolved_method}:skipped",
                    "ML_FORECAST_SKIPPED",
                    audit,
                )
            return {
                "status": "SKIPPED",
                "run_id": run_id,
                "snapshot_id": snapshot_id,
                "forecast_batch_id": batch_id,
                "method": resolved_method,
                "reason": str(exc),
                "audit": audit,
            }

        ack = {"inserted": 0} if dry_run else self.client.post_forecast_batch(batch_payload)
        if not dry_run:
            self.journal.mark_delivered(self.client.base_url, "forecast", run_id, batch_id)
        audit["replayed"] = replayed
        audit["target_expired"] = False
        audit["inserted"] = ack.get("inserted")
        if emit_event and not dry_run:
            event_type = "ML_FORECAST_FALLBACK" if audit.get("fallback") else "ML_FORECAST_PUBLISHED"
            audit["event_delivery"] = self._try_emit_event(
                run_id,
                f"ml:{run_id}:{snapshot_id}:{resolved_method}:published",
                event_type,
                audit,
                event_at_utc=audit.get("issued_at"),
            )
        return {
            "status": "OK",
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "forecast_batch_id": batch_id,
            "method": resolved_method,
            "row_count": len(batch_payload["rows"]),
            "inserted": ack.get("inserted"),
            "audit": audit,
            "batch": batch_payload,
        }

    def _try_emit_event(
        self,
        run_id: str,
        event_id: str,
        event_type: str,
        detail: dict,
        event_at_utc: str | None = None,
    ) -> dict:
        try:
            def build():
                return {
                    "event_id": event_id,
                    "event_at_utc": event_at_utc or stamp(),
                    "event_type": event_type,
                    "detail": dict(detail),
                }, {}

            binding = {"snapshot_http_sha256": detail.get("snapshot_http_sha256"),
                       "model_sha256": self.predictor.metadata["model_sha256"],
                       "event_type": event_type}
            payload, _, _ = self.journal.freeze(
                self.client.base_url, "event", run_id, event_id, binding, build,
            )
            self.client.post_ml_event(run_id, payload["event_id"], payload["event_type"],
                                      payload["detail"], payload["event_at_utc"])
            self.journal.mark_delivered(self.client.base_url, "event", run_id, event_id)
            return {"status": "DELIVERED"}
        except (DataApiError, ContractError) as exc:
            # Event recovery must not disguise a forecast that Data already ACKed.
            return {"status": "PENDING", "reason": str(exc)}
