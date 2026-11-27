import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Mapping

from optimization.planner import OptimizationController
from optimization.result_serializer import bind_decision_batch_id, build_decision_wire_payload

_INTERIM_APPLY_STATUSES = {"ACCEPTED", "APPLYING", "IN_PROGRESS", "PENDING", "QUEUED"}


class OptimizationServiceClient:
    def __init__(
        self,
        controller: OptimizationController,
        data_base_url: str,
        sdn_base_url: str,
        api_key: str | None = None,
        data_api_key: str | None = None,
        sdn_api_key: str | None = None,
        sdn_auth_header: str = "Authorization",
        sdn_auth_scheme: str = "Bearer",
        sdn_apply_endpoint: str = "/apply",
        sdn_status_endpoint: str = "/apply-status",
        sdn_state_endpoint: str = "/routing-state",
        timeout_sec: float = 2.0,
        max_retries: int = 2,
        retry_backoff_sec: float = 0.05,
        apply_poll_interval_sec: float = 0.05,
        apply_max_polls: int = 20,
    ) -> None:
        self.controller = controller
        self.data_base_url = data_base_url.rstrip("/")
        self.sdn_base_url = sdn_base_url.rstrip("/")
        self.data_api_key = data_api_key if data_api_key is not None else api_key
        self.sdn_api_key = sdn_api_key if sdn_api_key is not None else api_key
        self.sdn_auth_header = sdn_auth_header
        self.sdn_auth_scheme = sdn_auth_scheme
        self.sdn_apply_endpoint = sdn_apply_endpoint
        self.sdn_status_endpoint = sdn_status_endpoint
        self.sdn_state_endpoint = sdn_state_endpoint
        self.timeout_sec = timeout_sec
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_sec = max(0.0, float(retry_backoff_sec))
        self.apply_poll_interval_sec = max(0.0, float(apply_poll_interval_sec))
        self.apply_max_polls = max(1, int(apply_max_polls))

    def _build_data_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.data_api_key:
            headers["X-API-Key"] = self.data_api_key
        return headers

    def _build_sdn_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.sdn_api_key:
            headers[self.sdn_auth_header] = f"{self.sdn_auth_scheme} {self.sdn_api_key}".strip()
        return headers

    def _request_json_with_status(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any] | None = None,
        retryable: bool = False,
    ) -> tuple[int, dict[str, Any]]:
        data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
        attempts = self.max_retries if retryable else 0

        for attempt in range(attempts + 1):
            req = urllib.request.Request(url=url, data=data_bytes, headers=dict(headers), method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    status_code = int(getattr(resp, "status", 200))
                    raw = resp.read().decode("utf-8")
                    body = json.loads(raw) if raw else {}
                    if not isinstance(body, dict):
                        body = {"data": body}
                    return status_code, body
            except urllib.error.HTTPError as exc:
                body_text = exc.read().decode("utf-8", errors="replace")
                if retryable and exc.code >= 500 and attempt < attempts:
                    if self.retry_backoff_sec > 0:
                        time.sleep(self.retry_backoff_sec)
                    continue
                raise RuntimeError(f"HTTP {exc.code} từ {url}: {body_text}") from exc
            except urllib.error.URLError as exc:
                if retryable and attempt < attempts:
                    if self.retry_backoff_sec > 0:
                        time.sleep(self.retry_backoff_sec)
                    continue
                raise RuntimeError(f"Lỗi kết nối tới {url}: {exc.reason}") from exc

        raise RuntimeError(f"Không thể hoàn tất yêu cầu tới {url}")

    def get_forecast_batch(self, run_id: str, forecast_batch_id: str) -> dict[str, Any]:
        safe_run = urllib.parse.quote(str(run_id).strip(), safe="")
        safe_batch = urllib.parse.quote(str(forecast_batch_id).strip(), safe="")
        url = f"{self.data_base_url}/v1/runs/{safe_run}/forecasts/{safe_batch}"
        _, body = self._request_json_with_status("GET", url, headers=self._build_data_headers(), retryable=True)
        return body

    def get_routing_state(self, endpoint_path: str | None = None) -> dict[str, Any]:
        url = f"{self.sdn_base_url}{endpoint_path or self.sdn_state_endpoint}"
        _, body = self._request_json_with_status("GET", url, headers=self._build_sdn_headers(), retryable=True)
        return body

    def post_decision_batch_to_data(
        self,
        decision_batch: Mapping[str, Any],
        endpoint_path: str = "/v1/optimization/decisions",
    ) -> dict[str, Any]:
        frozen_wire_payload = build_decision_wire_payload(decision_batch)
        _, body = self._request_json_with_status(
            "POST",
            f"{self.data_base_url}{endpoint_path}",
            headers=self._build_data_headers(),
            payload=frozen_wire_payload,
            retryable=True,
        )
        return body

   def poll_apply_status(
        self,
        decision_batch_id: str,
        status_endpoint: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        base_ep = (status_endpoint or self.sdn_status_endpoint).strip()
        safe_id = urllib.parse.quote(str(decision_batch_id).strip(), safe="")
        safe_run = urllib.parse.quote(str(run_id).strip(), safe="") if run_id and str(run_id).strip() else ""

        if "{run_id}" in base_ep or "{decision_batch_id}" in base_ep:
            base_ep = base_ep.format(run_id=safe_run, decision_batch_id=safe_id)

        if base_ep.startswith("http://") or base_ep.startswith("https://"):
            url = base_ep
        else:
            path_prefix = base_ep if base_ep.startswith("/") else f"/{base_ep}"
            url = f"{self.sdn_base_url}{path_prefix}"

        parsed = urllib.parse.urlsplit(url)
        query_params = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        if safe_run and "run_id" not in query_params and safe_run not in parsed.path:
            query_params["run_id"] = str(run_id).strip()
        if "decision_batch_id" not in query_params and safe_id not in parsed.path:
            query_params["decision_batch_id"] = str(decision_batch_id).strip()

        url = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query_params), parsed.fragment)
        )

        last_body: dict[str, Any] = {"status": "PENDING", "decision_batch_id": decision_batch_id}
        for _ in range(self.apply_max_polls):
            if self.apply_poll_interval_sec > 0:
                time.sleep(self.apply_poll_interval_sec)
            code, body = self._request_json_with_status("GET", url, headers=self._build_sdn_headers(), retryable=True)
            last_body = body
            st = str(body.get("status", "")).upper()
            if code != 202 and st not in _INTERIM_APPLY_STATUSES:
                return body

        out = dict(last_body)
        out.setdefault("decision_batch_id", decision_batch_id)
        out["status"] = "TIMEOUT"
        return out

    def post_apply_request_to_sdn(
        self,
        apply_request: Mapping[str, Any],
        endpoint_path: str | None = None,
        status_endpoint: str | None = None,
    ) -> dict[str, Any]:
        url = f"{self.sdn_base_url}{endpoint_path or self.sdn_apply_endpoint}"
        code, body = self._request_json_with_status(
            "POST", url, headers=self._build_sdn_headers(), payload=dict(apply_request), retryable=False
        )
        st = str(body.get("status", "")).upper()
        if code == 202 or st in _INTERIM_APPLY_STATUSES:
            poll_target = (
                body.get("poll_url")
                or body.get("status_url")
                or status_endpoint
                or self.sdn_status_endpoint
            )
            return self.poll_apply_status(
                decision_batch_id=str(apply_request["decision_batch_id"]),
                status_endpoint=str(poll_target),
                run_id=str(apply_request.get("run_id") or "") or None,
            )
        return body

    def execute_tick(
        self,
        *,
        u_current: float,
        forecast_data: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        forecast_batch_id: str | None = None,
        routing_state: Mapping[str, Any] | None = None,
        now_utc: datetime | None = None,
        decision_batch_id: str | None = None,
        apply_deadline_utc: str | None = None,
        tick_id: int | str | None = None,
        tick_at_utc: str | None = None,
        data_decision_endpoint: str = "/v1/optimization/decisions",
        sdn_apply_endpoint: str | None = None,
        sdn_status_endpoint: str | None = None,
        fetch_routing_state_after_apply: bool = False,
    ) -> dict[str, Any]:
        effective_routing = routing_state if routing_state is not None else self.get_routing_state()
        effective_forecast = forecast_data
        if effective_forecast is None:
            if not run_id or not forecast_batch_id:
                raise ValueError("Cần truyền forecast_data hoặc cả run_id và forecast_batch_id")
            effective_forecast = self.get_forecast_batch(run_id=run_id, forecast_batch_id=forecast_batch_id)

        plan = self.controller.step(
            u_current=u_current,
            forecast_data=effective_forecast,
            routing_state=effective_routing,
            now_utc=now_utc,
            decision_batch_id=decision_batch_id,
            apply_deadline_utc=apply_deadline_utc,
            tick_id=tick_id,
            tick_at_utc=tick_at_utc,
        )

        decision_batch = plan.get("decision_batch")
        if decision_batch is not None:
            data_ack = self.post_decision_batch_to_data(decision_batch, endpoint_path=data_decision_endpoint)
            plan["data_ack"] = data_ack
            acked_id = data_ack.get("decision_batch_id")
            if isinstance(acked_id, str) and acked_id.strip():
                bind_decision_batch_id(plan, acked_id.strip())

        apply_request = plan.get("apply_request")
        if plan["status"] == "APPLY_REQUESTED" and apply_request is not None:
            apply_result = self.post_apply_request_to_sdn(
                apply_request,
                endpoint_path=sdn_apply_endpoint,
                status_endpoint=sdn_status_endpoint,
            )
            plan["apply_result"] = apply_result
            pub_state: Mapping[str, Any] | None = None
            if isinstance(apply_result, Mapping) and isinstance(apply_result.get("published_routing_state"), Mapping):
                pub_state = apply_result["published_routing_state"]
            elif fetch_routing_state_after_apply:
                pub_state = self.get_routing_state()

            plan["hold_down_update"] = self.controller.ingest_apply_result(
                apply_result,
                now_utc=now_utc,
                published_routing_state=pub_state,
            )

        return plan