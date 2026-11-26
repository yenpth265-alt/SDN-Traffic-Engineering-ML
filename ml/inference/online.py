"""Online Data API client and tick runner for ML forecasting (v1.4)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

from ml.common import ContractError, require, stamp, utc, whole
from ml.inference.predict import Predictor, unwrap_snapshot

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
    return method


def build_forecast_batch(
    run_id: str,
    snapshot_id: str,
    rows: list[dict],
    forecast_batch_id: str | None = None,
    method: str = "rf",
) -> dict:
    """Wrap 48 x H seven-field Prediction rows into Data API's ForecastBatch payload."""
    require(bool(rows), "Cannot build ForecastBatch from empty rows")
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

    def __init__(self, artifact_path, client: DataApiClient):
        self.predictor = Predictor(artifact_path)
        self.client = client
        self._runs: dict[str, dict] = {}

    def get_run(self, run_id: str, *, refresh: bool = False) -> dict:
        if refresh or run_id not in self._runs:
            self._runs[run_id] = self.client.get_run(run_id)
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
        """Fetch snapshot, run Predictor, and POST ForecastBatch to Data API."""
        run_info = self.get_run(run_id)
        resolved_method = resolve_method(method, run_info)
        batch_id = forecast_batch_id or f"{run_id}:{snapshot_id}:{resolved_method}"
        snapshot_payload, raw_sha256 = self.client.get_snapshot(run_id, snapshot_id)
        try:
            context = build_context(
                run_info,
                snapshot_payload,
                self.predictor,
                max_snapshot_age_s=max_snapshot_age_s,
                forecast_timeout_ms=forecast_timeout_ms,
                fallback_policy=fallback_policy,
                fallback_epsilon=fallback_epsilon,
            )
            rows = self.predictor.predict_batch(
                snapshot_payload, context, method=resolved_method, clock=clock
            )
        except ContractError as exc:
            audit = dict(self.predictor.last_audit)
            audit.setdefault("status", "INVALID")
            audit.setdefault("reason", str(exc))
            audit["snapshot_http_sha256"] = raw_sha256
            if emit_event and not dry_run:
                self._try_emit_event(
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

        batch_payload = build_forecast_batch(
            run_id,
            snapshot_id,
            rows,
            forecast_batch_id=batch_id,
            method=resolved_method,
        )
        ack = {"inserted": 0} if dry_run else self.client.post_forecast_batch(batch_payload)
        audit = dict(self.predictor.last_audit)
        audit["snapshot_http_sha256"] = raw_sha256
        audit["forecast_batch_id"] = batch_id
        audit["inserted"] = ack.get("inserted")
        if emit_event and not dry_run:
            event_type = "ML_FORECAST_FALLBACK" if audit.get("fallback") else "ML_FORECAST_PUBLISHED"
            self._try_emit_event(
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
            "row_count": len(rows),
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
    ) -> None:
        try:
            self.client.post_ml_event(run_id, event_id, event_type, detail, event_at_utc)
        except DataApiError:
            # Diagnostic event failure must not mask the primary forecast decision.
            pass
