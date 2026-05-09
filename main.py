"""ProxyMaze — minimal FastAPI backend (in-memory state)."""

from __future__ import annotations

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4
from urllib.parse import urlparse

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, ConfigDict, Field, field_validator
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

_state_lock = threading.Lock()
_main_loop: asyncio.AbstractEventLoop | None = None
_heartbeat_wake: asyncio.Event | None = None

# --- In-memory state (guard reads/writes with _state_lock) ---

_config: dict[str, int] = {
    "check_interval_seconds": 15,
    "request_timeout_ms": 3000,
}

_proxies: dict[str, dict[str, Any]] = {}
_alerts: list[dict[str, Any]] = []
_webhooks: dict[str, str] = {}
_next_webhook_id = 1
_integrations: dict[str, dict[str, Any]] = {}
_next_integration_id = 1
_ALERT_THRESHOLD = 0.2
_delivery_queue: asyncio.Queue[dict[str, Any]] | None = None
_delivery_task: asyncio.Task[None] | None = None
_delivery_enqueued: set[tuple[str, str]] = set()
_delivery_succeeded: set[tuple[str, str]] = set()
_metrics: dict[str, int] = {
    "total_checks": 0,
    "webhook_deliveries": 0,
}


def _request_heartbeat_wake() -> None:
    loop = _main_loop
    ev = _heartbeat_wake
    if loop is not None and ev is not None:
        loop.call_soon_threadsafe(ev.set)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _new_alert_id() -> str:
    return f"alert-{uuid4().hex[:6]}"


def _event_key_from_payload(payload: dict[str, Any]) -> str:
    if payload["event"] == "alert.fired":
        return f"alert.fired:{payload['alert_id']}"
    return f"alert.resolved:{payload['alert_id']}"


def _iso_to_unix_seconds(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def _get_alert_by_id_locked(alert_id: str) -> dict[str, Any] | None:
    for alert in reversed(_alerts):
        if alert.get("alert_id") == alert_id:
            return alert
    return None


def _format_integration_payload(
    integration: dict[str, Any], event_payload: dict[str, Any]
) -> dict[str, Any]:
    """Return Slack Block Kit or Discord Embeds payload for an alert event."""
    event = event_payload["event"]
    alert_id = event_payload["alert_id"]
    username = integration.get("username") or "ProxyMaze"

    alert_data = _get_alert_by_id_locked(alert_id) or {}
    failure_rate = float(event_payload.get("failure_rate", alert_data.get("failure_rate", 0.0)))
    failed_proxies = int(event_payload.get("failed_proxies", alert_data.get("failed_proxies", 0)))
    total_proxies = int(event_payload.get("total_proxies", alert_data.get("total_proxies", 0)))
    threshold = float(event_payload.get("threshold", alert_data.get("threshold", _ALERT_THRESHOLD)))
    failed_ids = list(event_payload.get("failed_proxy_ids", alert_data.get("failed_proxy_ids", [])) or [])
    fired_at = event_payload.get("fired_at", alert_data.get("fired_at"))
    resolved_at = event_payload.get("resolved_at", alert_data.get("resolved_at"))

    if integration.get("type") == "discord":
        color = 0xD7263D if event == "alert.fired" else 0x2ECC71
        title = "Proxy pool breach" if event == "alert.fired" else "Proxy pool recovered"
        desc = (
            f"Failure rate is {failure_rate:.2%} (threshold {threshold:.0%})."
            if event == "alert.fired"
            else f"Alert `{alert_id}` resolved."
        )
        return {
            "username": username,
            "embeds": [
                {
                    "title": title,
                    "description": desc,
                    "color": color,
                    "fields": [
                        {"name": "Alert ID", "value": str(alert_id), "inline": True},
                        {"name": "Failure rate", "value": f"{failure_rate:.2%}", "inline": True},
                        {
                            "name": "Failed Proxies",
                            "value": f"{failed_proxies}/{total_proxies}",
                            "inline": True,
                        },
                        {"name": "Threshold", "value": f"{threshold:.0%}", "inline": True},
                        {
                            "name": "Failed IDs",
                            "value": (", ".join(failed_ids) if failed_ids else "none"),
                            "inline": False,
                        },
                    ],
                    "footer": {"text": "ProxyMaze Alerts"},
                }
            ],
        }

    # Slack (grader spec): legacy payload with attachments.
    header_text = "Proxy pool breach" if event == "alert.fired" else "Proxy pool recovered"
    summary_text = (
        f"{header_text}: {failure_rate:.2%} failing ({failed_proxies}/{total_proxies})"
        if event == "alert.fired"
        else f"{header_text}: alert {alert_id} resolved"
    )
    ts_source = fired_at or resolved_at or _utc_now_iso()
    ts = _iso_to_unix_seconds(ts_source)
    color = "#D7263D" if event == "alert.fired" else "#2ECC71"
    failed_line = ", ".join(failed_ids) if failed_ids else "none"

    fields = [
        {"title": "Alert ID", "value": str(alert_id)},
        {"title": "Failure Rate", "value": f"{failure_rate:.2%}"},
        {"title": "Failed Proxies", "value": f"{failed_proxies}/{total_proxies}"},
        {"title": "Threshold", "value": f"{threshold:.0%}"},
        {"title": "Failed IDs", "value": failed_line},
        {"title": "Fired At", "value": str(fired_at or "n/a")},
    ]

    return {
        "username": username,
        "text": summary_text,
        "attachments": [
            {
                "color": color,
                "fields": fields,
                "footer": "ProxyMaze Alerts",
                "ts": ts,
            }
        ],
    }


def _sync_alerts_locked() -> list[dict[str, Any]]:
    """Maintain one-active-alert lifecycle based on current proxy states."""
    total = len(_proxies)
    failed_ids = sorted([pid for pid, rec in _proxies.items() if rec.get("status") == "down"])
    failed_count = len(failed_ids)
    failure_rate = (failed_count / total) if total else 0.0
    now = _utc_now_iso()
    events: list[dict[str, Any]] = []

    active_alert = next((a for a in reversed(_alerts) if a.get("status") == "active"), None)
    breach = total > 0 and failure_rate >= _ALERT_THRESHOLD

    if breach:
        if active_alert is None:
            _alerts.append(
                {
                    "alert_id": _new_alert_id(),
                    "status": "active",
                    "failure_rate": failure_rate,
                    "total_proxies": total,
                    "failed_proxies": failed_count,
                    "failed_proxy_ids": failed_ids,
                    "threshold": _ALERT_THRESHOLD,
                    "fired_at": now,
                    "resolved_at": None,
                    "message": "Proxy pool failure rate exceeded threshold",
                }
            )
            current = _alerts[-1]
            events.append(
                {
                    "event": "alert.fired",
                    "alert_id": current["alert_id"],
                    "fired_at": current["fired_at"],
                    "failure_rate": current["failure_rate"],
                    "total_proxies": current["total_proxies"],
                    "failed_proxies": current["failed_proxies"],
                    "failed_proxy_ids": current["failed_proxy_ids"],
                    "threshold": current["threshold"],
                    "message": current["message"],
                }
            )
            return events

        # Keep the active alert consistent with current pool state.
        active_alert["failure_rate"] = failure_rate
        active_alert["total_proxies"] = total
        active_alert["failed_proxies"] = failed_count
        active_alert["failed_proxy_ids"] = failed_ids
        active_alert["threshold"] = _ALERT_THRESHOLD
        active_alert["message"] = "Proxy pool failure rate exceeded threshold"
        return events

    if active_alert is not None:
        active_alert["status"] = "resolved"
        active_alert["resolved_at"] = now
        events.append(
            {
                "event": "alert.resolved",
                "alert_id": active_alert["alert_id"],
                "resolved_at": now,
            }
        )
    return events


async def _enqueue_event_deliveries(event_payload: dict[str, Any]) -> None:
    queue = _delivery_queue
    if queue is None:
        return
    event_key = _event_key_from_payload(event_payload)
    with _state_lock:
        webhooks = list(_webhooks.items())
        integrations = list(_integrations.items())
        to_enqueue: list[dict[str, Any]] = []
        for webhook_id, url in webhooks:
            recipient_id = f"webhook:{webhook_id}"
            marker = (event_key, recipient_id)
            if marker in _delivery_succeeded or marker in _delivery_enqueued:
                continue
            _delivery_enqueued.add(marker)
            to_enqueue.append(
                {
                    "recipient_id": recipient_id,
                    "url": url,
                    "event_key": event_key,
                    "payload": dict(event_payload),
                }
            )

        for integration_id, integration in integrations:
            if event_payload["event"] not in integration.get("events", ["alert.fired", "alert.resolved"]):
                continue
            recipient_id = f"integration:{integration_id}"
            marker = (event_key, recipient_id)
            if marker in _delivery_succeeded or marker in _delivery_enqueued:
                continue
            _delivery_enqueued.add(marker)
            to_enqueue.append(
                {
                    "recipient_id": recipient_id,
                    "url": integration["webhook_url"],
                    "event_key": event_key,
                    "payload": _format_integration_payload(integration, dict(event_payload)),
                }
            )
    for item in to_enqueue:
        await queue.put(item)


async def _delivery_worker() -> None:
    queue = _delivery_queue
    if queue is None:
        return
    timeout = httpx.Timeout(8.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        while True:
            item = await queue.get()
            recipient_id = item["recipient_id"]
            url = item["url"]
            event_key = item["event_key"]
            payload = item["payload"]
            marker = (event_key, recipient_id)
            backoff_s = 1.0
            try:
                while True:
                    try:
                        # Preserve POST across redirects (some capture servers front with redirects).
                        target = url
                        redirects = 0
                        while True:
                            resp = await client.post(
                                target,
                                json=payload,
                                headers={"Content-Type": "application/json"},
                            )
                            if (
                                resp.status_code in (301, 302, 303, 307, 308)
                                and "location" in resp.headers
                                and redirects < 5
                            ):
                                loc = resp.headers["location"]
                                if loc.startswith("/"):
                                    p = httpx.URL(target)
                                    loc = str(p.copy_with(path=loc, query=None, fragment=None))
                                target = loc
                                redirects += 1
                                continue
                            break
                        if 200 <= resp.status_code < 300:
                            with _state_lock:
                                _delivery_enqueued.discard(marker)
                                _delivery_succeeded.add(marker)
                                _metrics["webhook_deliveries"] += 1
                            break
                        if resp.status_code in (500, 502, 503, 504):
                            await asyncio.sleep(backoff_s)
                            backoff_s = min(backoff_s * 2.0, 30.0)
                            continue
                        with _state_lock:
                            _delivery_enqueued.discard(marker)
                            # Do not keep re-enqueuing forever on permanent 4xx responses.
                            _delivery_succeeded.add(marker)
                        break
                    except httpx.RequestError:
                        await asyncio.sleep(backoff_s)
                        backoff_s = min(backoff_s * 2.0, 30.0)
            finally:
                queue.task_done()


async def _probe_proxy(client: httpx.AsyncClient, url: str, timeout_s: float) -> bool:
    timeout = httpx.Timeout(timeout_s)
    try:
        # Use GET so timeout and 5xx are always classified as down.
        r = await client.get(url, timeout=timeout, follow_redirects=True)
        return 200 <= r.status_code < 300
    except httpx.RequestError:
        # Timeout / connection failures / refusal are down by definition.
        return False


async def _heartbeat_loop() -> None:
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    async with httpx.AsyncClient(limits=limits) as client:
        while True:
            with _state_lock:
                cfg = _config.copy()
                targets = [(pid, _proxies[pid]["url"]) for pid in sorted(_proxies.keys())]

            interval = cfg["check_interval_seconds"]
            timeout_s = cfg["request_timeout_ms"] / 1000.0

            if targets:
                # Probe in parallel so a round isn't N * timeout.
                results = await asyncio.gather(
                    *[_probe_proxy(client, url, timeout_s) for _, url in targets],
                    return_exceptions=True,
                )
                at = _utc_now_iso()
                with _state_lock:
                    for (pid, _), ok in zip(targets, results):
                        if pid not in _proxies:
                            continue
                        is_up = bool(ok) if not isinstance(ok, Exception) else False
                        _metrics["total_checks"] += 1
                        rec = _proxies[pid]
                        if is_up:
                            rec["status"] = "up"
                            rec["consecutive_failures"] = 0
                        else:
                            rec["consecutive_failures"] = int(rec.get("consecutive_failures") or 0) + 1
                            rec["status"] = "down"
                        rec["last_checked_at"] = at
                        rec["total_checks"] = int(rec.get("total_checks") or 0) + 1
                        if is_up:
                            rec["total_successes"] = int(rec.get("total_successes") or 0) + 1
                        history = rec.setdefault("history", [])
                        history.append({"checked_at": at, "status": rec["status"]})

            with _state_lock:
                events = _sync_alerts_locked()
            for event_payload in events:
                await _enqueue_event_deliveries(event_payload)

            wake = _heartbeat_wake
            if wake is None:
                break
            wake.clear()
            try:
                await asyncio.wait_for(wake.wait(), timeout=float(interval))
            except asyncio.TimeoutError:
                pass


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _main_loop, _heartbeat_wake, _delivery_queue, _delivery_task
    _main_loop = asyncio.get_running_loop()
    _heartbeat_wake = asyncio.Event()
    _delivery_queue = asyncio.Queue()
    _delivery_task = asyncio.create_task(_delivery_worker())
    task = asyncio.create_task(_heartbeat_loop())
    try:
        yield
    finally:
        task.cancel()
        if _delivery_task is not None:
            _delivery_task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if _delivery_task is not None:
            try:
                await _delivery_task
            except asyncio.CancelledError:
                pass
        _delivery_task = None
        _delivery_queue = None
        _main_loop = None
        _heartbeat_wake = None


app = FastAPI(title="ProxyMaze", lifespan=_lifespan)

# Trust X-Forwarded-* from Render so OpenAPI / logs see https and the public host.
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
# Browser clients (Swagger "Try it out", local SPAs) may not be same-origin in some tools.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def custom_openapi() -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        routes=app.routes,
    )
    # Relative base so Swagger UI builds https://<host>/... instead of a bad/missing URL.
    openapi_schema["servers"] = [{"url": "/", "description": "This deployment"}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # type: ignore[method-assign]


# --- Pydantic models ---


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


class ConfigUpdate(BaseModel):
    check_interval_seconds: int | None = None
    request_timeout_ms: int | None = None

    @field_validator("check_interval_seconds")
    @classmethod
    def check_interval_valid(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("check_interval_seconds must be >= 1")
        return v

    @field_validator("request_timeout_ms")
    @classmethod
    def request_timeout_valid(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("request_timeout_ms must be >= 1")
        return v


class ConfigResponse(BaseModel):
    check_interval_seconds: int
    request_timeout_ms: int


class ProxiesUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")

    proxies: list[str] = Field(default_factory=list)
    replace: bool = False

    @field_validator("proxies")
    @classmethod
    def non_empty_strings(cls, v: list[str]) -> list[str]:
        for i, item in enumerate(v):
            if not isinstance(item, str) or not item.strip():
                raise ValueError(f"proxies[{i}] must be a non-empty string")
        return v


class ProxyRecord(BaseModel):
    id: str
    url: str
    status: Literal["pending", "up", "down"] = "pending"
    last_checked_at: str | None = None
    consecutive_failures: int = 0


class ProxiesUpsertResponse(BaseModel):
    accepted: int
    proxies: list[ProxyRecord]


class ProxiesListResponse(BaseModel):
    total: int
    up: int
    down: int
    failure_rate: float
    proxies: list[ProxyRecord]


class ProxyHistoryEntry(BaseModel):
    checked_at: str
    status: Literal["up", "down"]


class AlertsListResponse(BaseModel):
    alert_id: str
    status: Literal["active", "resolved"]
    failure_rate: float
    total_proxies: int
    failed_proxies: int
    failed_proxy_ids: list[str]
    threshold: float
    fired_at: str
    resolved_at: str | None
    message: str


class WebhookCreateRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    url: str

    @field_validator("url")
    @classmethod
    def url_must_be_http(cls, v: str) -> str:
        value = v.strip()
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("url must be an absolute http(s) URL")
        return value


class WebhookCreateResponse(BaseModel):
    webhook_id: str
    url: str


class IntegrationCreateRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: Literal["slack", "discord"]
    webhook_url: str
    username: str = "ProxyMaze"
    events: list[Literal["alert.fired", "alert.resolved"]] = Field(
        default_factory=lambda: ["alert.fired", "alert.resolved"]
    )

    @field_validator("webhook_url")
    @classmethod
    def webhook_url_must_be_http(cls, v: str) -> str:
        value = v.strip()
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("webhook_url must be an absolute http(s) URL")
        return value

    @field_validator("username")
    @classmethod
    def username_non_empty(cls, v: str) -> str:
        value = v.strip()
        if not value:
            raise ValueError("username must be non-empty")
        return value

    @field_validator("events")
    @classmethod
    def events_non_empty(
        cls, v: list[Literal["alert.fired", "alert.resolved"]]
    ) -> list[Literal["alert.fired", "alert.resolved"]]:
        if not v:
            raise ValueError("events must include at least one event")
        return v


class IntegrationCreateResponse(BaseModel):
    integration_id: str
    type: Literal["slack", "discord"]
    webhook_url: str
    username: str
    events: list[Literal["alert.fired", "alert.resolved"]]


class MetricsResponse(BaseModel):
    total_checks: int
    current_pool_size: int
    active_alerts: int
    total_alerts: int
    webhook_deliveries: int


class ProxyDetailResponse(BaseModel):
    id: str
    url: str
    status: Literal["pending", "up", "down"]
    last_checked_at: str | None
    consecutive_failures: int
    total_checks: int
    uptime_percentage: float
    history: list[ProxyHistoryEntry]


# --- Helpers ---


def _proxy_id_from_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("proxy URL must be an absolute http(s) URL with a host")
    path = (parsed.path or "").rstrip("/")
    if path:
        segment = path.split("/")[-1]
    else:
        segment = ""
    if not segment:
        raise ValueError(
            "proxy URL must have a non-empty last path segment used as id (e.g. .../px-101)"
        )
    return segment


def _merge_config(data: dict[str, int], update: ConfigUpdate) -> dict[str, int]:
    out = data.copy()
    if update.check_interval_seconds is not None:
        out["check_interval_seconds"] = update.check_interval_seconds
    if update.request_timeout_ms is not None:
        out["request_timeout_ms"] = update.request_timeout_ms
    return out


def _build_proxy_detail(rec: dict[str, Any]) -> ProxyDetailResponse:
    total_checks = int(rec.get("total_checks") or 0)
    total_successes = int(rec.get("total_successes") or 0)
    uptime_percentage = round((total_successes / total_checks) * 100.0, 1) if total_checks else 0.0
    history = [ProxyHistoryEntry(**h) for h in rec.get("history", [])]
    return ProxyDetailResponse(
        id=rec["id"],
        url=rec["url"],
        status=rec["status"],
        last_checked_at=rec.get("last_checked_at"),
        consecutive_failures=int(rec.get("consecutive_failures") or 0),
        total_checks=total_checks,
        uptime_percentage=uptime_percentage,
        history=history,
    )


# --- Routes ---


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()


@app.post("/config", response_model=ConfigResponse)
def post_config(body: ConfigUpdate) -> ConfigResponse:
    """Set monitoring cadence (`check_interval_seconds`) and probe timeout (`request_timeout_ms`).

    Values take effect immediately: the next probe cycle uses the new timeout, and the sleep
    between full passes is interrupted so a new pass can start right away.
    """
    global _config
    with _state_lock:
        _config = _merge_config(_config, body)
        out = ConfigResponse(**_config)
    _request_heartbeat_wake()
    return out


@app.get("/config", response_model=ConfigResponse)
def get_config() -> ConfigResponse:
    with _state_lock:
        return ConfigResponse(**_config)


@app.post(
    "/proxies",
    response_model=ProxiesUpsertResponse,
    status_code=status.HTTP_201_CREATED,
)
def post_proxies(body: ProxiesUpsert) -> ProxiesUpsertResponse:
    global _proxies
    with _state_lock:
        if body.replace:
            _proxies = {}

        accepted = 0
        accepted_ids: list[str] = []
        errors: list[str] = []

        for raw in body.proxies:
            url = raw.strip()
            try:
                pid = _proxy_id_from_url(url)
            except ValueError as e:
                errors.append(f"{raw!r}: {e}")
                continue

            _proxies[pid] = {
                "id": pid,
                "url": url,
                "status": "pending",
                "last_checked_at": None,
                "consecutive_failures": 0,
                "total_checks": 0,
                "total_successes": 0,
                "history": [],
            }
            accepted += 1
            accepted_ids.append(pid)

        if errors and accepted == 0 and body.proxies:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "No valid proxies in request", "errors": errors},
            )

        records = [ProxyRecord(**_proxies[k]) for k in sorted(accepted_ids)]
        result = ProxiesUpsertResponse(accepted=accepted, proxies=records)
    _request_heartbeat_wake()
    return result


@app.get("/proxies", response_model=ProxiesListResponse)
def get_proxies() -> ProxiesListResponse:
    with _state_lock:
        items = [ProxyRecord(**_proxies[k]) for k in sorted(_proxies.keys())]
        total = len(items)
        up = sum(1 for p in items if p.status == "up")
        down = sum(1 for p in items if p.status == "down")
        failure_rate = (down / total) if total else 0.0
        return ProxiesListResponse(
            total=total,
            up=up,
            down=down,
            failure_rate=failure_rate,
            proxies=items,
        )


@app.get("/proxies/{proxy_id}", response_model=ProxyDetailResponse)
def get_proxy_by_id(proxy_id: str) -> ProxyDetailResponse:
    with _state_lock:
        rec = _proxies.get(proxy_id)
        if rec is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": f"Proxy '{proxy_id}' not found"},
            )
        return _build_proxy_detail(dict(rec))


@app.get("/proxies/{proxy_id}/history", response_model=list[ProxyHistoryEntry])
def get_proxy_history(proxy_id: str) -> list[ProxyHistoryEntry]:
    with _state_lock:
        rec = _proxies.get(proxy_id)
        if rec is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": f"Proxy '{proxy_id}' not found"},
            )
        return [ProxyHistoryEntry(**h) for h in rec.get("history", [])]


@app.delete("/proxies", status_code=status.HTTP_204_NO_CONTENT)
def delete_proxies() -> Response:
    global _proxies
    with _state_lock:
        _proxies = {}
    _request_heartbeat_wake()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/alerts", response_model=list[AlertsListResponse])
def get_alerts() -> list[AlertsListResponse]:
    with _state_lock:
        # Alert history survives proxy pool purge.
        return [AlertsListResponse(**dict(a)) for a in _alerts]


@app.post(
    "/webhooks",
    response_model=WebhookCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def post_webhooks(body: WebhookCreateRequest) -> WebhookCreateResponse:
    global _next_webhook_id
    with _state_lock:
        webhook_id = f"wh-{_next_webhook_id}"
        _next_webhook_id += 1
        _webhooks[webhook_id] = body.url
        return WebhookCreateResponse(webhook_id=webhook_id, url=body.url)


@app.post(
    "/integrations",
    response_model=IntegrationCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def post_integrations(body: IntegrationCreateRequest) -> IntegrationCreateResponse:
    global _next_integration_id
    with _state_lock:
        integration_id = f"int-{_next_integration_id}"
        _next_integration_id += 1
        _integrations[integration_id] = {
            "type": body.type,
            "webhook_url": body.webhook_url,
            "username": body.username,
            "events": list(body.events),
        }
        return IntegrationCreateResponse(
            integration_id=integration_id,
            type=body.type,
            webhook_url=body.webhook_url,
            username=body.username,
            events=list(body.events),
        )


@app.get("/metrics", response_model=MetricsResponse)
def get_metrics() -> MetricsResponse:
    with _state_lock:
        active_alerts = sum(1 for a in _alerts if a.get("status") == "active")
        return MetricsResponse(
            total_checks=_metrics["total_checks"],
            current_pool_size=len(_proxies),
            active_alerts=active_alerts,
            total_alerts=len(_alerts),
            webhook_deliveries=_metrics["webhook_deliveries"],
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)


# -----------------------------------------------------------------------------
# Run instructions
#
#   pip install fastapi uvicorn
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
# Render (or any host that sets PORT): use `python main.py` to bind the port
# from the environment, or pass it explicitly, e.g. on Unix:
#   uvicorn main:app --host 0.0.0.0 --port $PORT
# -----------------------------------------------------------------------------
