from datetime import timedelta
import re
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, BeforeValidator, ConfigDict, Field, model_validator

UTC_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)")


def utc_text(value):
    if not isinstance(value, str) or UTC_PATTERN.fullmatch(value) is None:
        raise ValueError("Expected an RFC3339 UTC timestamp with at most six fractional digits")
    return value


UTC = Annotated[AwareDatetime, BeforeValidator(utc_text)]
ID = Annotated[str, Field(min_length=1, max_length=256)]
Count = Annotated[int, Field(strict=True, ge=0, le=2**63 - 1)]
Counter = Annotated[int, Field(strict=True, ge=0, le=2**64 - 1)]
Bin = Annotated[int, Field(strict=True, ge=-(2**31), lt=2**31)]
Rate = Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)]
Positive = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Window(Message):
    flow_group_id: ID
    window_start_utc: UTC
    window_end_utc: UTC

    @model_validator(mode="after")
    def check_bin(self):
        if self.window_end_utc - self.window_start_utc != timedelta(seconds=1):
            raise ValueError("Expected a one-second window")
        return self


class Demand(Window):
    run_id: ID
    event_id: ID
    bin_index: Bin
    source: Literal["network"] = "network"
    requested_mbps: Rate | None
    reason: ID | None = None
    released_at_utc: UTC

    @model_validator(mode="after")
    def check_release(self):
        if self.released_at_utc < self.window_end_utc:
            raise ValueError("Demand was released before the window closed")
        if self.requested_mbps is None and self.reason is None:
            raise ValueError("Missing requested_mbps requires a reason")
        return self


class Traffic(Window):
    sent_bytes: Count | None
    received_bytes: Count | None
    sent_packets: Count | None
    received_packets: Count | None


class TrafficBatch(Message):
    run_id: ID
    rows: Annotated[list[Traffic], Field(min_length=1, max_length=4096)]


class Link(Message):
    link_id: ID
    measure_switch: ID
    measure_port: Annotated[int, Field(strict=True, gt=0, lt=2**31)]
    tx_bytes_total: Counter | None = None
    delta_bytes: Counter | None
    delta_t_s: Positive | None
    port_tx_mbps: Rate | None
    counter_status: ID
    # Optional metadata already emitted by the Network collector.
    from_node: ID | None = None
    to_node: ID | None = None
    is_fabric: bool | None = None
    utilization: Rate | None = None
    tx_drops_delta: Counter | None = None


class LinkBatch(Message):
    run_id: ID
    batch_id: ID
    sampled_at_utc: UTC
    bin_index: Bin | None = None
    interval_s: Positive | None = None
    sample_count: Count | None = None
    samples: Annotated[list[Link], Field(max_length=4096)]

    @model_validator(mode="after")
    def check_batch(self):
        ids = {row.link_id for row in self.samples}
        if not ids and self.sample_count != 0:
            raise ValueError("An empty collector batch must declare sample_count=0")
        if len(ids) != len(self.samples) or self.sample_count not in (None, len(ids)):
            raise ValueError("Invalid link batch count or duplicate directed link")
        return self


class Route(Message):
    flow_group_id: ID
    path_id: ID | None
    confirmed_at_utc: UTC | None
    verification_status: Literal["VERIFIED", "UNKNOWN", "INCONSISTENT"]

    @model_validator(mode="after")
    def check_confirmation(self):
        verified = self.verification_status == "VERIFIED"
        if verified != (self.path_id is not None) or verified != (self.confirmed_at_utc is not None):
            raise ValueError("Verified state requires path and confirmation time")
        return self


class Routing(Message):
    run_id: ID
    state_version: Count
    published_at_utc: UTC
    rows: Annotated[list[Route], Field(min_length=1, max_length=4096)]


class HistoryRow(Message):
    flow_group_id: ID
    bin_index: Bin
    requested_mbps: Rate | None
    available_at_utc: UTC


class Snapshot(Message):
    run_id: ID
    snapshot_id: ID
    history_cutoff_utc: UTC
    cutoff_bin: Annotated[int, Field(strict=True, ge=0, lt=2**31)]
    snapshot_as_of_utc: UTC
    state_version: Count
    schema_version: ID
    quality_status: Literal["VALID", "INCOMPLETE", "STALE", "INVALID"]
    history: Annotated[list[HistoryRow], Field(max_length=100000)]
    routing_state: Annotated[list[Route], Field(min_length=1, max_length=4096)]


class Prediction(Message):
    run_id: ID
    flow_group_id: ID
    issued_at: UTC
    forecast_start: Annotated[int, Field(strict=True, ge=0, lt=2**31 - 1)]
    forecast_end: Annotated[int, Field(strict=True, gt=0, lt=2**31)]
    predicted_mbps: Rate
    source: ID


class ForecastBatch(Message):
    run_id: ID
    forecast_batch_id: ID
    snapshot_id: ID
    rows: Annotated[list[Prediction], Field(min_length=1, max_length=100000)]
