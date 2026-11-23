"""
SnapshotCreator for SDN Controller Phase 4 (IF05, REQ-03, REQ-06, REQ-16, AC-S10, AC-D02, AC-D05, AC-D10).

Responsibilities:
1. Build and publish online immutable `Snapshot` objects via `create_snapshot(run_id, cutoff, lookback_L)`
   combining causal historical `requested_mbps` across all 48 canonical flows and verified live
   routing state from `StateManager`.
2. Receive demand history via an abstract `DemandHistoryProviderProtocol` because the live
   `released_demand_to_sdn` channel (`REQ-03`) is `[OPEN / MUST-LOCK]`.
3. Enforce the three strict causality inequalities on every observation in `X = [t - L*, t)`:
   - `window_end_utc <= history_cutoff_utc`
   - `released_at_utc <= snapshot_as_of_utc`
   - `available_at_utc <= snapshot_as_of_utc`
4. Prevent future demand leakage: observations with `window_end_utc > history_cutoff_utc`
   (bins `>= cutoff`) are strictly ignored so future demand changes never affect `Snapshot`
   or `Snapshot.content_hash` (`AC-D05`, `TC-I02`).
5. Reject forbidden demand sources (`goodput_mbps`, `port_tx_mbps`, `port_counter`,
   `receiver_goodput`, `unreleased_demand_plan`) (`REQ-03`, `AC-S10`).
6. Never substitute `0.0` for missing data (`0.0` is genuine zero demand; missing or
   un-released data is `requested_mbps=None`, `valid=False`, `reason='DATA_MISSING'`).
7. Never mark a snapshot `VALID` if history is incomplete (`< 48 * L*` valid bins),
   stale (`> T_stale_s`), or routing state is incomplete/unverified (`routing_state_complete=False`).
8. Guarantee snapshot immutability: once created, a `Snapshot` is frozen and its
   SHA-256 `content_hash` is deterministic. Data only validates, stores, and serves it (`AC-D10`).
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from datetime import timedelta
import math
from typing import Mapping, Optional, Protocol, Sequence, runtime_checkable

from sdn.controller.state_manager import (
    StateManager,
    StateValidationError,
    format_utc_timestamp,
    parse_utc_timestamp,
)
from sdn.controller.topology_registry import TopologyLookupError, TopologyRegistry
from sdn.schemas.snapshot_models import (
    FORBIDDEN_DEMAND_SOURCE_KINDS,
    DemandObservationRecord,
    DemandSourceKind,
    LinkSampleRef,
    OriginRefs,
    Snapshot,
    SnapshotHistoryItem,
    SnapshotProvenance,
    SnapshotQualityStatus,
    compute_snapshot_content_hash,
)
from sdn.schemas.state_models import ReasonCode


class SnapshotCreationError(Exception):
    """Base exception for SnapshotCreator errors."""


class ForbiddenDemandSourceError(SnapshotCreationError, ValueError):
    """
    Raised when a caller or provider attempts to supply `goodput_mbps`, `port_tx_mbps`,
    `port_counter`, or `unreleased_demand_plan` as `requested_mbps` (REQ-03, AC-S10).
    """


@runtime_checkable
class DemandHistoryProviderProtocol(Protocol):
    """
    Abstract interface for feeding causal released demand (`requested_mbps`) to SDN (`REQ-03`).

    Because the runtime transport for `released_demand_to_sdn` is `[OPEN / MUST-LOCK]`,
    `SnapshotCreator` depends only on this protocol.
    """

    @property
    def provider_name(self) -> str:
        """Identifier of the demand history provider implementation."""
        ...

    @property
    def demand_channel_status(self) -> str:
        """Contract lock status (e.g., `'MUST_LOCK_MOCK_PROVIDER'`)."""
        ...

    def get_observations(
        self,
        run_id: str,
    ) -> Sequence[DemandObservationRecord]:
        """Return candidate demand observation records for `run_id`."""
        ...


@runtime_checkable
class LinkTelemetryProviderProtocol(Protocol):
    """Optional abstract interface for directional link telemetry sample references (`IF05`)."""

    def get_link_sample_refs(
        self,
        run_id: str,
    ) -> Sequence[LinkSampleRef]:
        """Return candidate directional link sample references for `run_id`."""
        ...


class MockReleasedDemandProvider:
    """
    In-memory mock provider for `released_demand_to_sdn` on Windows (`PASS_MOCK`).
    Explicitly tagged `demand_channel_status='MUST_LOCK_MOCK_PROVIDER'`.
    """

    def __init__(
        self,
        *,
        provider_name: str = "MockReleasedDemandProvider",
        demand_channel_status: str = "MUST_LOCK_MOCK_PROVIDER",
    ) -> None:
        self._provider_name = provider_name
        self._demand_channel_status = demand_channel_status
        self._records_by_run: dict[str, list[DemandObservationRecord]] = {}

    @property
    def provider_name(self) -> str:
        return self._provider_name

    @property
    def demand_channel_status(self) -> str:
        return self._demand_channel_status

    def add_observations(
        self, observations: Sequence[DemandObservationRecord]
    ) -> None:
        """Add one or more `DemandObservationRecord` items into the mock store."""
        for obs in observations:
            bucket = self._records_by_run.setdefault(obs.run_id, [])
            bucket.append(replace(obs))

    def clear_run(self, run_id: str) -> None:
        """Clear all stored observations for `run_id`."""
        self._records_by_run.pop(run_id, None)

    def populate_uniform_window(
        self,
        *,
        run_id: str,
        flow_group_ids: Sequence[str],
        origin_refs: OriginRefs,
        start_bin: int,
        end_bin_exclusive: int,
        base_mbps: float = 10.0,
        release_delay_s: float = 0.0,
        available_delay_s: float = 0.0,
        source_kind: str = DemandSourceKind.RELEASED_DEMAND.value,
    ) -> list[DemandObservationRecord]:
        """
        Helper for Windows tests: populate deterministic causal demand observations across
        `flow_group_ids` for bins `k in [start_bin, end_bin_exclusive)`.
        """
        meas_dt = parse_utc_timestamp(origin_refs.measurement_started_at_utc)
        delta_s = origin_refs.delta_s
        created: list[DemandObservationRecord] = []

        for k in range(start_bin, end_bin_exclusive):
            w_start_dt = meas_dt + timedelta(seconds=k * delta_s)
            w_end_dt = meas_dt + timedelta(seconds=(k + 1) * delta_s)
            rel_dt = w_end_dt + timedelta(seconds=release_delay_s)
            avail_dt = w_end_dt + timedelta(seconds=available_delay_s)

            w_start_utc = format_utc_timestamp(w_start_dt)
            w_end_utc = format_utc_timestamp(w_end_dt)
            rel_utc = format_utc_timestamp(rel_dt)
            avail_utc = format_utc_timestamp(avail_dt)

            for idx, fid in enumerate(flow_group_ids):
                val = (
                    0.0
                    if float(base_mbps) == 0.0
                    else round(float(base_mbps) + (idx * 0.1) + (k * 0.01), 4)
                )
                rec = DemandObservationRecord(
                    run_id=run_id,
                    flow_group_id=fid,
                    bin_index=k,
                    window_start_utc=w_start_utc,
                    window_end_utc=w_end_utc,
                    requested_mbps=val,
                    released_at_utc=rel_utc,
                    available_at_utc=avail_utc,
                    release_record_id=f"{run_id}:{fid}:bin:{k}",
                    source_kind=source_kind,
                    valid=True,
                    reason=None,
                )
                created.append(rec)

        self.add_observations(created)
        return created

    def get_observations(
        self,
        run_id: str,
    ) -> tuple[DemandObservationRecord, ...]:
        """Return a defensive tuple of observations for `run_id`."""
        return tuple(self._records_by_run.get(run_id, ()))

    def ingest_network_release_event(
        self,
        event: Mapping[str, object],
        *,
        origin_refs: Optional[OriginRefs] = None,
        available_at_utc: Optional[str] = None,
    ) -> DemandObservationRecord:
        """
        Convert and store a `DemandRelease` JSON event emitted by `network/traffic_generator/sender.py`
        (`event_id`, `run_id`, `flow_group_id`, `bin_index`, `window_start_utc`, `window_end_utc`,
        `requested_mbps`, `released_at_utc`) into a canonical `DemandObservationRecord` (`Phase 7.7`).
        """
        run_id = str(event["run_id"]).strip()
        fid = str(event["flow_group_id"]).strip()
        w_start = format_utc_timestamp(str(event["window_start_utc"]))
        w_end = format_utc_timestamp(str(event["window_end_utc"]))
        rel_utc = format_utc_timestamp(str(event["released_at_utc"]))
        avail_utc = format_utc_timestamp(
            available_at_utc if available_at_utc is not None else str(event.get("available_at_utc", rel_utc))
        )
        if origin_refs is not None:
            meas_dt = parse_utc_timestamp(origin_refs.measurement_started_at_utc)
            w_start_dt = parse_utc_timestamp(w_start)
            delta_s = origin_refs.delta_s if origin_refs.delta_s > 0 else 1
            bin_idx = int(round((w_start_dt - meas_dt).total_seconds())) // delta_s
        else:
            bin_idx = int(event["bin_index"])  # type: ignore[arg-type]

        raw_req = event.get("requested_mbps")
        req_mbps = float(raw_req) if raw_req is not None else None  # type: ignore[arg-type]
        rec_id = str(
            event.get("release_record_id")
            or event.get("event_id")
            or f"network:{run_id}:demand:{fid}:{bin_idx}"
        )
        src_kind = str(event.get("source_kind", DemandSourceKind.RELEASED_DEMAND.value))
        rec = DemandObservationRecord(
            run_id=run_id,
            flow_group_id=fid,
            bin_index=bin_idx,
            window_start_utc=w_start,
            window_end_utc=w_end,
            requested_mbps=req_mbps,
            released_at_utc=rel_utc,
            available_at_utc=avail_utc,
            release_record_id=rec_id,
            source_kind=src_kind,
            valid=(req_mbps is not None),
            reason=None if req_mbps is not None else ReasonCode.DATA_MISSING.value,
        )
        self.add_observations((rec,))
        return rec


class MockLinkTelemetryProvider:
    """In-memory mock provider for directional link telemetry sample references (`IF05`)."""

    def __init__(self) -> None:
        self._samples_by_run: dict[str, list[LinkSampleRef]] = {}

    def add_samples(self, run_id: str, samples: Sequence[LinkSampleRef]) -> None:
        bucket = self._samples_by_run.setdefault(run_id, [])
        bucket.extend(samples)

    def ingest_network_telemetry_batch(
        self,
        batch_event: Mapping[str, object],
        *,
        available_at_utc: Optional[str] = None,
    ) -> tuple[LinkSampleRef, ...]:
        """
        Convert and store a `LinkSampleBatch` JSON event emitted by
        `network/telemetry/telemetry_collector.py` (`batch_id`, `run_id`, `bin_index`,
        `sampled_at_utc`, `samples`) into canonical `LinkSampleRef` records (`Phase 7.7`).
        """
        run_id = str(batch_event["run_id"]).strip()
        batch_id = str(batch_event.get("batch_id", f"network:{run_id}:telemetry"))
        sampled_utc = format_utc_timestamp(str(batch_event["sampled_at_utc"]))
        avail_utc = format_utc_timestamp(
            available_at_utc if available_at_utc is not None else sampled_utc
        )
        raw_samples = batch_event.get("samples", ())
        created: list[LinkSampleRef] = []
        if isinstance(raw_samples, Sequence):
            for s in raw_samples:
                if not isinstance(s, Mapping):
                    continue
                lid = str(s["link_id"]).strip()
                status = str(s.get("counter_status", "OK")).strip().upper()
                is_valid = status == "OK"
                ref = LinkSampleRef(
                    sample_id=f"{batch_id}:{lid}",
                    link_id=lid,
                    sample_at_utc=sampled_utc,
                    available_at_utc=avail_utc,
                    valid=is_valid,
                )
                created.append(ref)
        self.add_samples(run_id, created)
        return tuple(created)

    def get_link_sample_refs(self, run_id: str) -> tuple[LinkSampleRef, ...]:
        return tuple(self._samples_by_run.get(run_id, ()))


@dataclass(frozen=True)
class SnapshotCreatorConfig:
    """
    Configuration for `SnapshotCreator` (`IF05`, `REQ-03`, `REQ-06`, `AC-S10`).
    All pilot parameters (`delta_s`, `stale_threshold_s`, `config_version`, `config_hash`)
    are configurable rather than hard-coded.
    """

    delta_s: int = 1
    stale_threshold_s: float = 2.0
    config_version: str = "1.0"
    config_hash: str = "sdn_config_v1_hash"
    strict_raise_on_forbidden_source: bool = False
    default_origin_refs: OriginRefs = OriginRefs(
        run_started_at_utc="2026-11-12T08:59:00Z",
        traffic_started_at_utc="2026-11-12T08:59:00Z",
        measurement_started_at_utc="2026-11-12T09:00:00Z",
        warmup_s=60,
        delta_s=1,
    )


class SnapshotCreator:
    """
    Creates and freezes causal online `Snapshot` objects for the SDN Controller (`IF05`, `AC-S10`).

    Strictly enforces:
    - `window_end_utc <= history_cutoff_utc`
    - `released_at_utc <= snapshot_as_of_utc`
    - `available_at_utc <= snapshot_as_of_utc`
    - Rejection of `goodput_mbps`, `port_tx_mbps`, `port_counter`, or `unreleased_demand_plan`
    - Preservation of genuine `0.0` demand vs. `None` (`valid=False`, `reason='DATA_MISSING'`)
    - Verification of complete 48-flow `routing_state` via `StateManager`
    - Immutability of closed snapshots (late data arrivals never mutate a created `Snapshot`)
    """

    def __init__(
        self,
        topology_registry: TopologyRegistry,
        state_manager: StateManager,
        demand_provider: DemandHistoryProviderProtocol,
        config: Optional[SnapshotCreatorConfig] = None,
        *,
        link_telemetry_provider: Optional[LinkTelemetryProviderProtocol] = None,
    ) -> None:
        self._topology = topology_registry
        self._state_manager = state_manager
        self._demand_provider = demand_provider
        self._config = config or SnapshotCreatorConfig()
        self._link_provider = link_telemetry_provider
        self._origins_by_run: dict[str, OriginRefs] = {}
        self._closed_snapshots: dict[tuple[str, str], Snapshot] = {}

    @property
    def topology(self) -> TopologyRegistry:
        return self._topology

    @property
    def state_manager(self) -> StateManager:
        return self._state_manager

    @property
    def demand_provider(self) -> DemandHistoryProviderProtocol:
        return self._demand_provider

    @property
    def config(self) -> SnapshotCreatorConfig:
        return self._config

    def set_run_origin_refs(self, run_id: str, origin_refs: OriginRefs) -> None:
        """Register explicit `OriginRefs` (`run_started_at_utc`, `traffic_started_at_utc`, `measurement_started_at_utc`) for `run_id`."""
        parse_utc_timestamp(origin_refs.run_started_at_utc)
        parse_utc_timestamp(origin_refs.traffic_started_at_utc)
        parse_utc_timestamp(origin_refs.measurement_started_at_utc)
        self._origins_by_run[run_id] = origin_refs

    def get_run_origin_refs(self, run_id: str) -> OriginRefs:
        """Return registered `OriginRefs` for `run_id` or the configured default."""
        return self._origins_by_run.get(run_id, self._config.default_origin_refs)

    def get_closed_snapshot(
        self, run_id: str, snapshot_id: str
    ) -> Optional[Snapshot]:
        """Return a previously created immutable `Snapshot` by `(run_id, snapshot_id)`."""
        return self._closed_snapshots.get((run_id, snapshot_id))

    @staticmethod
    def _is_forbidden_source(source_kind: Optional[str]) -> bool:
        if source_kind is None:
            return True
        cleaned = source_kind.strip()
        if not cleaned:
            return True
        if cleaned in FORBIDDEN_DEMAND_SOURCE_KINDS or cleaned.lower() in FORBIDDEN_DEMAND_SOURCE_KINDS:
            return True
        return cleaned != DemandSourceKind.RELEASED_DEMAND.value

    def create_snapshot(
        self,
        run_id: str,
        cutoff: int | str,
        lookback_L: int,
        *,
        snapshot_as_of_utc: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        origin_refs: Optional[OriginRefs] = None,
    ) -> Snapshot:
        """
        Create an immutable online `Snapshot` for `(run_id, cutoff, lookback_L)` (`IF05`, `AC-S10`).

        Parameters:
        - `run_id`: Target run identifier (must be initialized in `StateManager`).
        - `cutoff`: Either an integer bin/second offset `t` from `measurement_started_at_utc`
          (e.g. `cutoff=10` for `[t - L*, t)`) or an RFC3339 UTC timestamp string.
        - `lookback_L`: Lookback horizon `L*` in bins (`>= 1`).
        - `snapshot_as_of_utc`: UTC timestamp when the snapshot is closed/published
          (defaults to `history_cutoff_utc`).
        - `snapshot_id`: Optional explicit snapshot ID (defaults to
          `f"snap:{run_id}:t{cutoff_offset_s}:L{lookback_L}:v{state_version}"`).
        - `origin_refs`: Optional `OriginRefs` override for `run_id`.
        """
        if not isinstance(run_id, str) or not run_id.strip():
            raise SnapshotCreationError("run_id must be a non-empty string.")
        if (
            isinstance(lookback_L, bool)
            or not isinstance(lookback_L, int)
            or lookback_L <= 0
        ):
            raise SnapshotCreationError(
                f"lookback_L must be a positive integer, got {lookback_L!r}."
            )

        try:
            self._state_manager.get_routing_state(run_id)
        except StateValidationError as exc:
            raise SnapshotCreationError(str(exc)) from exc

        eff_origins = origin_refs or self.get_run_origin_refs(run_id)
        delta_s = eff_origins.delta_s if eff_origins.delta_s > 0 else self._config.delta_s
        meas_start_dt = parse_utc_timestamp(eff_origins.measurement_started_at_utc)

        if isinstance(cutoff, int) and not isinstance(cutoff, bool):
            cutoff_offset_s = cutoff
            cutoff_dt = meas_start_dt + timedelta(seconds=cutoff_offset_s)
            history_cutoff_utc = format_utc_timestamp(cutoff_dt)
        elif isinstance(cutoff, str):
            cutoff_dt = parse_utc_timestamp(cutoff)
            history_cutoff_utc = format_utc_timestamp(cutoff_dt)
            cutoff_offset_s = int(round((cutoff_dt - meas_start_dt).total_seconds()))
        else:
            raise SnapshotCreationError(
                f"cutoff must be an int offset (seconds) or UTC RFC3339 string, got {type(cutoff).__name__}."
            )

        if cutoff_offset_s % delta_s != 0:
            raise SnapshotCreationError(
                f"cutoff_offset_s ({cutoff_offset_s}s) must be an integer multiple of delta_s ({delta_s}s)."
            )

        cutoff_bin = cutoff_offset_s // delta_s
        start_bin = cutoff_bin - lookback_L
        if start_bin < 0:
            raise SnapshotCreationError(
                f"cutoff_bin ({cutoff_bin}) < lookback_L ({lookback_L}); window [t - L*, t) "
                f"would precede measurement_started_at_utc."
            )

        window_start_utc = format_utc_timestamp(
            meas_start_dt + timedelta(seconds=start_bin * delta_s)
        )
        norm_as_of_utc = (
            format_utc_timestamp(snapshot_as_of_utc)
            if snapshot_as_of_utc is not None
            else history_cutoff_utc
        )
        as_of_dt = parse_utc_timestamp(norm_as_of_utc)
        if as_of_dt < cutoff_dt:
            raise SnapshotCreationError(
                f"snapshot_as_of_utc ('{norm_as_of_utc}') cannot precede "
                f"history_cutoff_utc ('{history_cutoff_utc}')."
            )

        routing_snap = self._state_manager.get_routing_state_as_of(
            run_id, norm_as_of_utc
        )
        causal_routing_history = self._state_manager.get_routing_history_in_window(
            run_id,
            window_start_utc,
            history_cutoff_utc,
            as_of_utc=norm_as_of_utc,
        )

        eff_snapshot_id = (
            snapshot_id.strip()
            if isinstance(snapshot_id, str) and snapshot_id.strip()
            else f"snap:{run_id}:t{cutoff_offset_s}:L{lookback_L}:v{routing_snap.state_version}"
        )

        # Immutability guard (AC-D05, AC-D10): if this exact (run_id, snapshot_id) was already
        # closed, return the frozen Snapshot so late arrivals can NEVER mutate it.
        existing = self._closed_snapshots.get((run_id, eff_snapshot_id))
        if existing is not None:
            return existing

        all_flows = self._topology.get_all_flow_groups()
        canonical_flow_ids = tuple(fg.flow_group_id for fg in all_flows)
        canonical_flow_set = frozenset(canonical_flow_ids)
        required_bins = tuple(range(start_bin, cutoff_bin))

        # Index candidate observations from provider
        raw_observations = self._demand_provider.get_observations(run_id)
        indexed_obs: dict[tuple[str, int], DemandObservationRecord] = {}
        invalid_source_detected = False

        for obs in raw_observations:
            if obs.run_id != run_id:
                continue
            if obs.flow_group_id not in canonical_flow_set:
                continue

            # Parse observation window timestamps
            try:
                obs_start_dt = parse_utc_timestamp(obs.window_start_utc)
                obs_end_dt = parse_utc_timestamp(obs.window_end_utc)
            except StateValidationError:
                continue

            if obs_end_dt <= obs_start_dt:
                continue

            # STRICT CAUSALITY RULE #1 (IF05, AC-S10, AC-D05):
            # Ignore any observation whose window_end_utc > history_cutoff_utc (future bins >= cutoff_bin).
            # This guarantees that adding/mutating future schedule rows never changes Snapshot or content_hash.
            if obs_end_dt > cutoff_dt or obs.bin_index >= cutoff_bin:
                continue
            if obs.bin_index < start_bin:
                continue

            # Check forbidden demand sources (REQ-03, AC-S10):
            # Reject goodput_mbps, port_tx_mbps, port_counter, unreleased_demand_plan
            if self._is_forbidden_source(obs.source_kind):
                if self._config.strict_raise_on_forbidden_source:
                    raise ForbiddenDemandSourceError(
                        f"Forbidden demand source '{obs.source_kind}' for flow '{obs.flow_group_id}' "
                        f"at bin {obs.bin_index}; SDN SnapshotCreator accepts ONLY 'RELEASED_DEMAND' (REQ-03)."
                    )
                invalid_source_detected = True

            indexed_obs[(obs.flow_group_id, obs.bin_index)] = obs

        history_items: list[SnapshotHistoryItem] = []
        missing_flow_set: set[str] = set()
        valid_bin_count = 0
        expected_bin_count = len(canonical_flow_ids) * lookback_L
        has_invalid_data_bin = invalid_source_detected

        for fid in canonical_flow_ids:
            for k in required_bins:
                expected_w_start_dt = meas_start_dt + timedelta(seconds=k * delta_s)
                expected_w_end_dt = meas_start_dt + timedelta(seconds=(k + 1) * delta_s)
                expected_w_start_utc = format_utc_timestamp(expected_w_start_dt)
                expected_w_end_utc = format_utc_timestamp(expected_w_end_dt)

                obs = indexed_obs.get((fid, k))
                if obs is None:
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=None,
                            available_at_utc=None,
                            valid=False,
                            reason=ReasonCode.DATA_MISSING.value,
                            release_record_id=None,
                        )
                    )
                    continue

                # Check forbidden source on this bin
                if self._is_forbidden_source(obs.source_kind):
                    has_invalid_data_bin = True
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=obs.released_at_utc,
                            available_at_utc=obs.available_at_utc,
                            valid=False,
                            reason=ReasonCode.DATA_INVALID.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                # Verify exact bin window alignment and window_end_utc <= cutoff_dt
                obs_w_start_dt = parse_utc_timestamp(obs.window_start_utc)
                obs_w_end_dt = parse_utc_timestamp(obs.window_end_utc)
                if (
                    obs_w_start_dt != expected_w_start_dt
                    or obs_w_end_dt != expected_w_end_dt
                    or obs_w_end_dt > cutoff_dt
                ):
                    has_invalid_data_bin = True
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=obs.released_at_utc,
                            available_at_utc=obs.available_at_utc,
                            valid=False,
                            reason=ReasonCode.DATA_INVALID.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                # STRICT CAUSALITY RULES #2 & #3 (IF05, AC-S10, REQ-03):
                # Both `released_at_utc` and `available_at_utc` MUST be present and `<= snapshot_as_of_utc`.
                if not obs.released_at_utc or not obs.available_at_utc:
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=obs.released_at_utc,
                            available_at_utc=obs.available_at_utc,
                            valid=False,
                            reason=ReasonCode.DATA_MISSING.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                try:
                    rel_dt = parse_utc_timestamp(obs.released_at_utc)
                    avail_dt = parse_utc_timestamp(obs.available_at_utc)
                except StateValidationError:
                    has_invalid_data_bin = True
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=obs.released_at_utc,
                            available_at_utc=obs.available_at_utc,
                            valid=False,
                            reason=ReasonCode.DATA_INVALID.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                if rel_dt > as_of_dt or avail_dt > as_of_dt:
                    # Late release / not yet available at snapshot_as_of_utc -> treat as missing at cutoff
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=format_utc_timestamp(rel_dt),
                            available_at_utc=format_utc_timestamp(avail_dt),
                            valid=False,
                            reason=ReasonCode.DATA_MISSING.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                # Validate requested_mbps (never fill 0.0 for None; preserve genuine 0.0)
                if not obs.valid or obs.requested_mbps is None:
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=format_utc_timestamp(rel_dt),
                            available_at_utc=format_utc_timestamp(avail_dt),
                            valid=False,
                            reason=obs.reason or ReasonCode.DATA_MISSING.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                if isinstance(obs.requested_mbps, bool):
                    has_invalid_data_bin = True
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=format_utc_timestamp(rel_dt),
                            available_at_utc=format_utc_timestamp(avail_dt),
                            valid=False,
                            reason=ReasonCode.DATA_INVALID.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                try:
                    val_float = float(obs.requested_mbps)
                except (TypeError, ValueError):
                    val_float = float("nan")

                if math.isnan(val_float) or math.isinf(val_float) or val_float < 0.0:
                    has_invalid_data_bin = True
                    missing_flow_set.add(fid)
                    history_items.append(
                        SnapshotHistoryItem(
                            flow_group_id=fid,
                            bin_index=k,
                            window_start_utc=expected_w_start_utc,
                            window_end_utc=expected_w_end_utc,
                            requested_mbps=None,
                            released_at_utc=format_utc_timestamp(rel_dt),
                            available_at_utc=format_utc_timestamp(avail_dt),
                            valid=False,
                            reason=ReasonCode.DATA_INVALID.value,
                            release_record_id=obs.release_record_id,
                        )
                    )
                    continue

                valid_bin_count += 1
                history_items.append(
                    SnapshotHistoryItem(
                        flow_group_id=fid,
                        bin_index=k,
                        window_start_utc=expected_w_start_utc,
                        window_end_utc=expected_w_end_utc,
                        requested_mbps=val_float,
                        released_at_utc=format_utc_timestamp(rel_dt),
                        available_at_utc=format_utc_timestamp(avail_dt),
                        valid=True,
                        reason=None,
                        release_record_id=obs.release_record_id,
                    )
                )

        # Filter optional directional link telemetry samples causally (`available_at_utc <= snapshot_as_of_utc`)
        causal_link_samples: list[LinkSampleRef] = []
        if self._link_provider is not None:
            for sample in self._link_provider.get_link_sample_refs(run_id):
                try:
                    self._topology.get_link(sample.link_id)
                    s_dt = parse_utc_timestamp(sample.sample_at_utc)
                    a_dt = parse_utc_timestamp(sample.available_at_utc)
                except (TopologyLookupError, StateValidationError):
                    continue
                if s_dt <= cutoff_dt and a_dt <= as_of_dt:
                    causal_link_samples.append(
                        LinkSampleRef(
                            sample_id=sample.sample_id,
                            link_id=sample.link_id,
                            sample_at_utc=format_utc_timestamp(s_dt),
                            available_at_utc=format_utc_timestamp(a_dt),
                            valid=sample.valid,
                        )
                    )

        # Evaluate overall SnapshotQualityStatus and reason (AC-S10)
        is_stale = (
            as_of_dt - cutoff_dt
        ).total_seconds() > self._config.stale_threshold_s
        ordered_missing_groups = tuple(
            fid for fid in canonical_flow_ids if fid in missing_flow_set
        )

        if not routing_snap.routing_state_complete:
            quality_status = SnapshotQualityStatus.INVALID_ROUTING_STATE
            snap_reason: Optional[str] = (
                ReasonCode.ROUTE_STATE_UNKNOWN.value
                if routing_snap.unknown_flow_group_ids
                else ReasonCode.RECONCILIATION_REQUIRED.value
            )
        elif has_invalid_data_bin:
            quality_status = SnapshotQualityStatus.INVALID_DATA
            snap_reason = ReasonCode.DATA_INVALID.value
        elif valid_bin_count < expected_bin_count or len(ordered_missing_groups) > 0:
            quality_status = SnapshotQualityStatus.INCOMPLETE_HISTORY
            snap_reason = ReasonCode.DATA_MISSING.value
        elif is_stale:
            quality_status = SnapshotQualityStatus.STALE
            snap_reason = ReasonCode.DATA_STALE.value
        else:
            quality_status = SnapshotQualityStatus.VALID
            snap_reason = None

        ev_mode_str = (
            routing_snap.evidence_mode.value
            if routing_snap.evidence_mode is not None
            else "UNVERIFIED"
        )
        provenance = SnapshotProvenance(
            producer="sdn_controller",
            demand_channel_status=self._demand_provider.demand_channel_status,
            demand_provider_name=self._demand_provider.provider_name,
            schema_version="1.0",
            evidence_mode=ev_mode_str,
        )

        provisional_snapshot = Snapshot(
            snapshot_id=eff_snapshot_id,
            run_id=run_id,
            snapshot_as_of_utc=norm_as_of_utc,
            history_cutoff_utc=history_cutoff_utc,
            cutoff_offset_s=cutoff_offset_s,
            lookback_L=lookback_L,
            delta_s=delta_s,
            state_version=routing_snap.state_version,
            config_version=routing_snap.config_version,
            config_hash=self._config.config_hash,
            topology_version=self._topology.topology_version,
            topology_hash=self._topology.topology_hash,
            content_hash="",
            origin_refs=copy.deepcopy(eff_origins),
            flow_group_ids=canonical_flow_ids,
            history=tuple(history_items),
            routing_state=routing_snap.routing_state,
            link_sample_refs=tuple(causal_link_samples),
            quality_status=quality_status,
            missing_groups=ordered_missing_groups,
            routing_state_complete=routing_snap.routing_state_complete,
            unknown_flow_group_ids=routing_snap.unknown_flow_group_ids,
            provenance=provenance,
            reason=snap_reason,
            expected_bin_count=expected_bin_count,
            valid_bin_count=valid_bin_count,
            routing_history=causal_routing_history,
        )

        digest = compute_snapshot_content_hash(provisional_snapshot)
        final_snapshot = replace(provisional_snapshot, content_hash=digest)
        self._closed_snapshots[(run_id, eff_snapshot_id)] = final_snapshot
        return final_snapshot
