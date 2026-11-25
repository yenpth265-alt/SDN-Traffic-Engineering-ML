"""
Data-backed DemandHistoryProvider for SDN Controller (`Phase 2`, Task A).

Reads causal released demand history directly from Data API v1:
    `GET /v1/runs/{run_id}/demand-history?cutoff=<UTC>&as_of=<UTC>&lookback=<int>&format=sdn`
authenticated via `X-API-Key` (role `SDN`).

Design guarantees:
- Implements `DemandHistoryProviderProtocol` without modifying SDN Core (`SnapshotCreator`).
- Preserves Data's original PostgreSQL-stamped `available_at_utc` and `released_at_utc` verbatim.
- Never silently merges Data API demand history with local SQLite `HTTPDemandReleaseProvider`.
- Never fabricates `0.0` for missing demand bins or falls back to local storage on failure.
- Fails closed on HTTP `401/403/404/409/422/5xx`, network errors, timeouts, malformed JSON,
  forbidden `source_kind`, or duplicate `(flow_group_id, bin_index)` conflicts, clearing any
  cached observations for that run so stale history is never reused.
- Never logs or leaks `api_key` in error messages or status summaries.
"""

from __future__ import annotations

import json
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

from sdn.controller.state_manager import format_utc_timestamp, parse_utc_timestamp
from sdn.monitoring.snapshot_creator import (
    DemandHistoryProviderProtocol,
    SnapshotCreator,
)
from sdn.schemas.snapshot_models import (
    FORBIDDEN_DEMAND_SOURCE_KINDS,
    DemandObservationRecord,
    DemandSourceKind,
    OriginRefs,
    Snapshot,
)
from sdn.schemas.state_models import ContractSerializationError

DATA_V1_DEMAND_HISTORY_PATH_TEMPLATE = "/v1/runs/{run_id}/demand-history"


class DataDemandHistoryError(RuntimeError):
    """Explicit fail-closed error raised when Data demand history cannot be fetched or validated."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        http_status: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.http_status = http_status


@dataclass(frozen=True)
class DataDemandHistoryConfig:
    """Configuration for `DataAPIDemandHistoryProvider`."""

    endpoint_base_url: Optional[str] = None
    api_key: Optional[str] = None
    timeout_s: float = 1.0
    format_name: str = "sdn"

    def __post_init__(self) -> None:
        if self.timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {self.timeout_s!r}.")
        if self.format_name != "sdn":
            raise ValueError(
                f"Unsupported demand-history format {self.format_name!r}; SDN requires 'sdn'."
            )


HttpGetFn = Callable[[str, dict[str, str], float], tuple[int, str]]


def _utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _default_http_get(
    url: str,
    headers: dict[str, str],
    timeout_s: float,
) -> tuple[int, str]:
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200))
            body = resp.read().decode("utf-8", errors="replace")
            return status, body
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        return int(exc.code), err_body


def _classify_http_error(status_code: int) -> str:
    if status_code in (401, 403):
        return "DATA_AUTH_FAILED"
    if status_code == 404:
        return "DATA_RUN_NOT_FOUND"
    if status_code == 409:
        return "DATA_RUN_CONFLICT"
    if status_code == 422:
        return "DATA_CONTRACT_VALIDATION_ERROR"
    if status_code >= 500:
        return "DATA_SERVER_ERROR"
    return f"DATA_HTTP_{status_code}"


class DataAPIDemandHistoryProvider(DemandHistoryProviderProtocol):
    """
    Data-backed `DemandHistoryProvider` reading `GET /v1/runs/{run_id}/demand-history?format=sdn`.

    Usage:
    1. Directly before `SnapshotCreator.create_snapshot(...)`:
       `provider.fetch_demand_history(run_id, history_cutoff_utc=..., snapshot_as_of_utc=..., lookback_L=...)`
       followed by `snapshot_creator.create_snapshot(...)`.
    2. Or via the convenience wrapper:
       `provider.create_snapshot_via_data(snapshot_creator, run_id=..., cutoff=..., lookback_L=...)`.
    """

    def __init__(
        self,
        config: Optional[DataDemandHistoryConfig] = None,
        *,
        http_get_fn: Optional[HttpGetFn] = None,
        provider_name: str = "DataAPIDemandHistoryProvider",
        demand_channel_status: str = "DATA_API_V1_PULL",
    ) -> None:
        self._config = config or DataDemandHistoryConfig()
        self._http_get_fn: HttpGetFn = http_get_fn or _default_http_get
        self._provider_name = provider_name
        self._demand_channel_status = demand_channel_status
        self._lock = threading.RLock()
        self._cached_by_run: dict[str, tuple[DemandObservationRecord, ...]] = {}
        self._last_query_by_run: dict[str, dict[str, Any]] = {}
        self._is_healthy: bool = True
        self._last_error: Optional[str] = None
        self._last_reason_code: Optional[str] = None
        self._last_http_status: Optional[int] = None
        self._last_fetch_utc: Optional[str] = None
        self._fetch_count: int = 0
        self._error_count: int = 0

    @property
    def provider_name(self) -> str:
        return self._provider_name

    @property
    def demand_channel_status(self) -> str:
        return self._demand_channel_status

    @property
    def config(self) -> DataDemandHistoryConfig:
        return self._config

    @property
    def is_healthy(self) -> bool:
        with self._lock:
            return self._is_healthy

    @property
    def last_error(self) -> Optional[str]:
        with self._lock:
            return self._last_error

    def _record_failure(
        self,
        run_id: str,
        reason_code: str,
        message: str,
        *,
        http_status: Optional[int] = None,
    ) -> DataDemandHistoryError:
        with self._lock:
            # Fail-closed: clear cached observations for this run so stale history is never reused.
            self._cached_by_run.pop(run_id, None)
            self._is_healthy = False
            self._last_reason_code = reason_code
            self._last_error = message
            self._last_http_status = http_status
            self._error_count += 1
        return DataDemandHistoryError(message, reason_code=reason_code, http_status=http_status)

    def fetch_demand_history(
        self,
        run_id: str,
        *,
        history_cutoff_utc: str,
        snapshot_as_of_utc: str,
        lookback_L: int,
    ) -> tuple[DemandObservationRecord, ...]:
        """
        Fetch causal demand history for `[cutoff - lookback_L, cutoff)` from Data API v1.

        Validates every row strictly and preserves Data's original `available_at_utc` and
        `released_at_utc`. Raises `DataDemandHistoryError` (fail-closed) on any transport,
        authentication, contract, or duplicate-conflict error.
        """
        if not isinstance(run_id, str) or not run_id.strip():
            raise self._record_failure(
                str(run_id), "INVALID_RUN_ID", "run_id must be a non-empty string."
            )
        if isinstance(lookback_L, bool) or not isinstance(lookback_L, int) or lookback_L <= 0:
            raise self._record_failure(
                run_id, "INVALID_LOOKBACK", f"lookback_L must be a positive integer, got {lookback_L!r}."
            )

        try:
            cutoff_dt = parse_utc_timestamp(history_cutoff_utc)
            as_of_dt = parse_utc_timestamp(snapshot_as_of_utc)
        except Exception as exc:
            raise self._record_failure(
                run_id,
                "INVALID_TIMESTAMP",
                f"Invalid cutoff/as_of UTC timestamp: {exc}",
            ) from exc
        if cutoff_dt > as_of_dt:
            raise self._record_failure(
                run_id,
                "INVALID_CAUSAL_BOUNDS",
                f"history_cutoff_utc ({history_cutoff_utc}) cannot exceed snapshot_as_of_utc ({snapshot_as_of_utc}).",
            )

        base_url = self._config.endpoint_base_url
        if not base_url or not base_url.strip():
            raise self._record_failure(
                run_id,
                "DATA_ENDPOINT_NOT_CONFIGURED",
                "Data API v1 endpoint_base_url is not configured for DataAPIDemandHistoryProvider.",
            )

        norm_cutoff = format_utc_timestamp(cutoff_dt)
        norm_as_of = format_utc_timestamp(as_of_dt)
        encoded_run_id = urllib.parse.quote(run_id, safe="")
        query = urllib.parse.urlencode(
            {
                "cutoff": norm_cutoff,
                "as_of": norm_as_of,
                "lookback": int(lookback_L),
                "format": self._config.format_name,
            }
        )
        url = f"{base_url.rstrip('/')}{DATA_V1_DEMAND_HISTORY_PATH_TEMPLATE.format(run_id=encoded_run_id)}?{query}"
        headers: dict[str, str] = {"Accept": "application/json"}
        if self._config.api_key:
            headers["X-API-Key"] = self._config.api_key

        try:
            status_code, body_text = self._http_get_fn(url, headers, float(self._config.timeout_s))
        except (TimeoutError, socket.timeout) as exc:
            raise self._record_failure(
                run_id,
                "DATA_TIMEOUT",
                f"Timed out fetching demand history from Data API v1 after {self._config.timeout_s}s: {exc}",
            ) from exc
        except urllib.error.URLError as exc:
            reason_code = (
                "DATA_TIMEOUT"
                if isinstance(getattr(exc, "reason", None), (TimeoutError, socket.timeout))
                else "DATA_UNREACHABLE"
            )
            raise self._record_failure(
                run_id,
                reason_code,
                f"Failed to reach Data API v1 demand-history endpoint: {exc.reason}",
            ) from exc
        except Exception as exc:
            raise self._record_failure(
                run_id,
                "DATA_TRANSPORT_ERROR",
                f"Unexpected error fetching demand history from Data API v1: {type(exc).__name__}: {exc}",
            ) from exc

        if status_code != 200:
            reason_code = _classify_http_error(int(status_code))
            raise self._record_failure(
                run_id,
                reason_code,
                f"Data API v1 demand-history returned HTTP {status_code}: {body_text[:240]}",
                http_status=int(status_code),
            )

        try:
            parsed = json.loads(body_text)
        except Exception as exc:
            raise self._record_failure(
                run_id,
                "DATA_MALFORMED_JSON",
                f"Data API v1 demand-history returned invalid JSON: {exc}",
                http_status=200,
            ) from exc

        if not isinstance(parsed, list):
            raise self._record_failure(
                run_id,
                "DATA_INVALID_SCHEMA",
                f"Expected JSON array from demand-history?format=sdn, got {type(parsed).__name__}.",
                http_status=200,
            )

        records_by_key: dict[tuple[str, int], DemandObservationRecord] = {}
        ordered_records: list[DemandObservationRecord] = []

        for idx, raw_item in enumerate(parsed):
            if not isinstance(raw_item, Mapping):
                raise self._record_failure(
                    run_id,
                    "DATA_INVALID_ROW",
                    f"Demand history row {idx} is not a JSON object.",
                    http_status=200,
                )
            raw_source = raw_item.get("source_kind", DemandSourceKind.RELEASED_DEMAND.value)
            if (
                not isinstance(raw_source, str)
                or raw_source in FORBIDDEN_DEMAND_SOURCE_KINDS
                or raw_source.lower() in FORBIDDEN_DEMAND_SOURCE_KINDS
                or raw_source != DemandSourceKind.RELEASED_DEMAND.value
            ):
                raise self._record_failure(
                    run_id,
                    "FORBIDDEN_DEMAND_SOURCE",
                    f"Demand history row {idx} carries forbidden source_kind={raw_source!r}.",
                    http_status=200,
                )
            try:
                rec = DemandObservationRecord.from_dict(raw_item, strict=True)
            except (ContractSerializationError, ValueError, KeyError, TypeError) as exc:
                raise self._record_failure(
                    run_id,
                    "DATA_ROW_CONTRACT_ERROR",
                    f"Demand history row {idx} failed v1.4 contract validation: {exc}",
                    http_status=200,
                ) from exc

            if rec.run_id != run_id:
                raise self._record_failure(
                    run_id,
                    "DATA_RUN_ID_MISMATCH",
                    f"Demand history row {idx} has run_id={rec.run_id!r}, expected {run_id!r}.",
                    http_status=200,
                )

            # Causal & window-bound defense-in-depth: reject future, stale, or non-1s rows
            w_start_dt = parse_utc_timestamp(rec.window_start_utc)
            w_end_dt = parse_utc_timestamp(rec.window_end_utc)
            window_start_bound = cutoff_dt - timedelta(seconds=int(lookback_L))
            if w_end_dt > cutoff_dt:
                raise self._record_failure(
                    run_id,
                    "DATA_CAUSALITY_VIOLATION",
                    f"Demand history row {idx} ({rec.flow_group_id}, bin={rec.bin_index}) "
                    f"has window_end_utc={rec.window_end_utc} > cutoff={norm_cutoff}.",
                    http_status=200,
                )
            if w_start_dt < window_start_bound:
                raise self._record_failure(
                    run_id,
                    "DATA_STALE_WINDOW_VIOLATION",
                    f"Demand history row {idx} ({rec.flow_group_id}, bin={rec.bin_index}) "
                    f"has window_start_utc={rec.window_start_utc} < window_start_bound={format_utc_timestamp(window_start_bound)}.",
                    http_status=200,
                )
            if w_end_dt - w_start_dt != timedelta(seconds=1):
                raise self._record_failure(
                    run_id,
                    "DATA_INVALID_BIN_DURATION",
                    f"Demand history row {idx} ({rec.flow_group_id}, bin={rec.bin_index}) "
                    f"does not have 1-second duration ({rec.window_start_utc}..{rec.window_end_utc}).",
                    http_status=200,
                )
            if rec.released_at_utc is not None and parse_utc_timestamp(rec.released_at_utc) > as_of_dt:
                raise self._record_failure(
                    run_id,
                    "DATA_CAUSALITY_VIOLATION",
                    f"Demand history row {idx} ({rec.flow_group_id}, bin={rec.bin_index}) "
                    f"has released_at_utc={rec.released_at_utc} > as_of={norm_as_of}.",
                    http_status=200,
                )
            if rec.available_at_utc is not None and parse_utc_timestamp(rec.available_at_utc) > as_of_dt:
                raise self._record_failure(
                    run_id,
                    "DATA_CAUSALITY_VIOLATION",
                    f"Demand history row {idx} ({rec.flow_group_id}, bin={rec.bin_index}) "
                    f"has available_at_utc={rec.available_at_utc} > as_of={norm_as_of}.",
                    http_status=200,
                )

            key = (rec.flow_group_id, rec.bin_index)
            existing = records_by_key.get(key)
            if existing is not None:
                if existing != rec:
                    raise self._record_failure(
                        run_id,
                        "DATA_DUPLICATE_CONFLICT",
                        f"Conflicting duplicate demand rows for (flow={rec.flow_group_id}, bin={rec.bin_index}).",
                        http_status=200,
                    )
                continue
            records_by_key[key] = rec
            ordered_records.append(rec)

        result_tuple = tuple(ordered_records)
        now_utc = _utc_now_rfc3339()
        with self._lock:
            self._cached_by_run[run_id] = result_tuple
            self._last_query_by_run[run_id] = {
                "cutoff": norm_cutoff,
                "as_of": norm_as_of,
                "lookback_L": int(lookback_L),
                "row_count": len(result_tuple),
                "fetched_at_utc": now_utc,
            }
            self._is_healthy = True
            self._last_error = None
            self._last_reason_code = None
            self._last_http_status = 200
            self._last_fetch_utc = now_utc
            self._fetch_count += 1
        return result_tuple

    def get_observations(self, run_id: str) -> Sequence[DemandObservationRecord]:
        """Return the last validated causal observation window fetched for `run_id` (fail-closed empty if none)."""
        with self._lock:
            return self._cached_by_run.get(run_id, ())

    def clear_cache(self, run_id: Optional[str] = None) -> None:
        """Clear cached observations for a single run or all runs."""
        with self._lock:
            if run_id is None:
                self._cached_by_run.clear()
                self._last_query_by_run.clear()
            else:
                self._cached_by_run.pop(run_id, None)
                self._last_query_by_run.pop(run_id, None)

    def create_snapshot_via_data(
        self,
        snapshot_creator: SnapshotCreator,
        *,
        run_id: str,
        cutoff: int | str,
        lookback_L: int,
        snapshot_as_of_utc: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        origin_refs: Optional[OriginRefs] = None,
    ) -> Snapshot:
        """
        Fetch causal demand history from Data API v1 and immediately create an immutable Snapshot.
        Fails closed (raises `DataDemandHistoryError`) if Data demand-history fetch fails.
        """
        eff_origins = origin_refs or snapshot_creator.get_run_origin_refs(run_id)
        meas_start_dt = parse_utc_timestamp(eff_origins.measurement_started_at_utc)
        delta_s = int(eff_origins.delta_s)
        if isinstance(cutoff, int) and not isinstance(cutoff, bool):
            cutoff_dt = meas_start_dt + timedelta(seconds=cutoff)
        else:
            cutoff_dt = parse_utc_timestamp(str(cutoff))
        history_cutoff_utc = format_utc_timestamp(cutoff_dt)
        norm_as_of = (
            format_utc_timestamp(parse_utc_timestamp(snapshot_as_of_utc))
            if snapshot_as_of_utc is not None
            else history_cutoff_utc
        )
        self.fetch_demand_history(
            run_id,
            history_cutoff_utc=history_cutoff_utc,
            snapshot_as_of_utc=norm_as_of,
            lookback_L=lookback_L,
        )
        return snapshot_creator.create_snapshot(
            run_id=run_id,
            cutoff=cutoff,
            lookback_L=lookback_L,
            snapshot_as_of_utc=norm_as_of,
            snapshot_id=snapshot_id,
            origin_refs=eff_origins,
        )

    def status_summary(self) -> dict[str, Any]:
        """Safe diagnostic status summary (never includes `api_key`)."""
        with self._lock:
            return {
                "provider_name": self._provider_name,
                "demand_channel_status": self._demand_channel_status,
                "endpoint_base_url": self._config.endpoint_base_url,
                "api_key_configured": bool(self._config.api_key),
                "timeout_s": float(self._config.timeout_s),
                "is_healthy": bool(self._is_healthy),
                "healthy": bool(self._is_healthy),
                "last_http_status": self._last_http_status,
                "last_reason_code": self._last_reason_code,
                "last_error": self._last_error,
                "last_fetch_utc": self._last_fetch_utc,
                "fetch_count": int(self._fetch_count),
                "error_count": int(self._error_count),
                "cached_runs": sorted(self._cached_by_run.keys()),
                "durable": False,
                "external_durable_owner": "DATA_POSTGRESQL",
                "source_mode": "data_v1",
            }

    def ledger_status(self) -> dict[str, Any]:
        """Compatibility hook for `/health` readiness inspection."""
        return self.status_summary()
