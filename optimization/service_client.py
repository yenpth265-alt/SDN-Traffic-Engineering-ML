import json
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Mapping

from optimization.planner import OptimizationController
from optimization.result_serializer import bind_decision_batch_id


class OptimizationServiceClient:
    """
    Cầu nối HTTP thực thi đúng phân quyền Mục 1 SDN & A08/A09:
    1. OptimizationController.step() tính toán DecisionBatch (48 dòng) & ApplyRequest (tối đa B dòng).
    2. Runner/Client POST DecisionBatch lên Data (/v1/optimization/decisions) và đợi Data ACK.
    3. Gắn đúng decision_batch_id từ Data ACK sang ApplyRequest (tự cập nhật payload_hash).
    4. Nếu status == APPLY_REQUESTED (B dương và có luồng đổi), POST ApplyRequest sang SDN và cập nhật Hold-down.
    """

    def __init__(
        self,
        controller: OptimizationController,
        data_base_url: str,
        sdn_base_url: str,
        api_key: str | None = None,
        timeout_sec: float = 2.0,
    ) -> None:
        self.controller = controller
        self.data_base_url = data_base_url.rstrip("/")
        self.sdn_base_url = sdn_base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_sec = timeout_sec

    def _request_json(
        self,
        method: str,
        url: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url=url, data=data_bytes, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {"http_status": resp.status}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} từ {url}: {body}") from exc

    def post_decision_batch_to_data(
        self,
        decision_batch: Mapping[str, Any],
        endpoint_path: str = "/v1/optimization/decisions",
    ) -> dict[str, Any]:
        return self._request_json("POST", f"{self.data_base_url}{endpoint_path}", decision_batch)

    def post_apply_request_to_sdn(
        self,
        apply_request: Mapping[str, Any],
        endpoint_path: str = "/v1/routing/apply",
    ) -> dict[str, Any]:
        return self._request_json("POST", f"{self.sdn_base_url}{endpoint_path}", apply_request)

    def execute_tick(
        self,
        *,
        u_current: float,
        forecast_data: Mapping[str, Any],
        routing_state: Mapping[str, Any],
        now_utc: datetime | None = None,
        decision_batch_id: str | None = None,
        apply_deadline_utc: str | None = None,
        tick_id: int | str | None = None,
        data_decision_endpoint: str = "/v1/optimization/decisions",
        sdn_apply_endpoint: str = "/v1/routing/apply",
    ) -> dict[str, Any]:
        plan = self.controller.step(
            u_current=u_current,
            forecast_data=forecast_data,
            routing_state=routing_state,
            now_utc=now_utc,
            decision_batch_id=decision_batch_id,
            apply_deadline_utc=apply_deadline_utc,
            tick_id=tick_id,
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
            plan["hold_down_update"] = self.controller.ingest_apply_result(
                apply_result, now_utc=now_utc
            )

        return plan