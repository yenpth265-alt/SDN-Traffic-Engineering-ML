import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Mapping

from optimization.planner import OptimizationController
from optimization.result_serializer import bind_decision_batch_id, build_decision_wire_payload


class OptimizationServiceClient:
    """
    Cầu nối HTTP:
    1. Lấy forecast READY qua GET /v1/runs/{run_id}/forecasts/{forecast_batch_id}.
    2. Tính DecisionBatch (48 rows) & ApplyRequest (1..B changes).
    3. POST wire payload của DecisionBatch lên Data (/v1/optimization/decisions) trước khi gọi SDN,
       giữ nguyên decision_batch_id và payload khi retry.
    4. Gắn decision_batch_id từ Data ACK sang ApplyRequest rồi POST sang SDN.
    """

    def __init__(
        self,
        controller: OptimizationController,
        data_base_url: str,
        sdn_base_url: str,
        api_key: str | None = None,
        timeout_sec: float = 2.0,
        max_retries: int = 2,
        retry_backoff_sec: float = 0.05,
    ) -> None:
        self.controller = controller
        self.data_base_url = data_base_url.rstrip("/")
        self.sdn_base_url = sdn_base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_sec = timeout_sec
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_sec = max(0.0, float(retry_backoff_sec))

    def _request_json(
        self,
        method: str,
        url: str,
        payload: Mapping[str, Any] | None = None,
        retryable: bool = False,
    ) -> dict[str, Any]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
        attempts = self.max_retries if retryable else 0

        for attempt in range(attempts + 1):
            req = urllib.request.Request(url=url, data=data_bytes, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw) if raw else {"http_status": resp.status}
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                if retryable and exc.code >= 500 and attempt < attempts:
                    if self.retry_backoff_sec > 0:
                        time.sleep(self.retry_backoff_sec)
                    continue
                raise RuntimeError(f"HTTP {exc.code} từ {url}: {body}") from exc
            except urllib.error.URLError as exc:
                if retryable and attempt < attempts:
                    if self.retry_backoff_sec > 0:
                        time.sleep(self.retry_backoff_sec)
                    continue
                raise RuntimeError(f"Lỗi kết nối tới {url}: {exc.reason}") from exc

        raise RuntimeError(f"Không thể hoàn tất yêu cầu tới {url}")

    def get_forecast_batch(
        self,
        run_id: str,
        forecast_batch_id: str,
    ) -> dict[str, Any]:
        safe_run = urllib.parse.quote(str(run_id).strip(), safe="")
        safe_batch = urllib.parse.quote(str(forecast_batch_id).strip(), safe="")
        url = f"{self.data_base_url}/v1/runs/{safe_run}/forecasts/{safe_batch}"
        return self._request_json("GET", url, payload=None, retryable=True)

    def post_decision_batch_to_data(
        self,
        decision_batch: Mapping[str, Any],
        endpoint_path: str = "/v1/optimization/decisions",
    ) -> dict[str, Any]:
        # Tách strict wire payload cho Data (extra="forbid") và giữ nguyên qua các lần retry
        frozen_wire_payload = build_decision_wire_payload(decision_batch)
        return self._request_json(
            "POST",
            f"{self.data_base_url}{endpoint_path}",
            payload=frozen_wire_payload,
            retryable=True,
        )

    def post_apply_request_to_sdn(
        self,
        apply_request: Mapping[str, Any],
        endpoint_path: str = "/v1/routing/apply",
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            f"{self.sdn_base_url}{endpoint_path}",
            payload=dict(apply_request),
            retryable=False,
        )

    def execute_tick(
        self,
        *,
        u_current: float,
        forecast_data: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        forecast_batch_id: str | None = None,
        routing_state: Mapping[str, Any],
        now_utc: datetime | None = None,
        decision_batch_id: str | None = None,
        apply_deadline_utc: str | None = None,
        tick_id: int | str | None = None,
        tick_at_utc: str | None = None,
        data_decision_endpoint: str = "/v1/optimization/decisions",
        sdn_apply_endpoint: str = "/v1/routing/apply",
    ) -> dict[str, Any]:
        effective_forecast = forecast_data
        if effective_forecast is None:
            if not run_id or not forecast_batch_id:
                raise ValueError("Cần truyền forecast_data hoặc cả run_id và forecast_batch_id")
            effective_forecast = self.get_forecast_batch(run_id=run_id, forecast_batch_id=forecast_batch_id)

        plan = self.controller.step(
            u_current=u_current,
            forecast_data=effective_forecast,
            routing_state=routing_state,
            now_utc=now_utc,
            decision_batch_id=decision_batch_id,
            apply_deadline_utc=apply_deadline_utc,
            tick_id=tick_id,
            tick_at_utc=tick_at_utc,
        )

        decision_batch = plan.get("decision_batch")
        if decision_batch is not None:
            data_ack = self.post_decision_batch_to_data(
                decision_batch, endpoint_path=data_decision_endpoint
            )
            plan["data_ack"] = data_ack
            acked_id = data_ack.get("decision_batch_id")
            if isinstance(acked_id, str) and acked_id.strip():
                bind_decision_batch_id(plan, acked_id.strip())

        apply_request = plan.get("apply_request")
        if plan["status"] == "APPLY_REQUESTED" and apply_request is not None:
            apply_result = self.post_apply_request_to_sdn(
                apply_request, endpoint_path=sdn_apply_endpoint
            )
            plan["apply_result"] = apply_result
            pub_state = (
                apply_result.get("published_routing_state")
                if isinstance(apply_result, Mapping)
                and isinstance(apply_result.get("published_routing_state"), Mapping)
                else None
            )
            plan["hold_down_update"] = self.controller.ingest_apply_result(
                apply_result,
                now_utc=now_utc,
                published_routing_state=pub_state,
            )

        return plan