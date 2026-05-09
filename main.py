"""ProxyMaze — minimal FastAPI backend (in-memory state)."""

from __future__ import annotations

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlparse

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, status
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


def _request_heartbeat_wake() -> None:
    loop = _main_loop
    ev = _heartbeat_wake
    if loop is not None and ev is not None:
        loop.call_soon_threadsafe(ev.set)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def _probe_proxy(client: httpx.AsyncClient, url: str, timeout_s: float) -> bool:
    timeout = httpx.Timeout(timeout_s)
    try:
        r = await client.head(url, timeout=timeout, follow_redirects=True)
        if r.status_code == 405:
            r = await client.get(url, timeout=timeout, follow_redirects=True)
        return bool(r.is_success or (200 <= r.status_code < 400))
    except httpx.RequestError:
        try:
            r = await client.get(url, timeout=timeout, follow_redirects=True)
            return bool(r.is_success or (200 <= r.status_code < 400))
        except httpx.RequestError:
            return False


async def _heartbeat_loop() -> None:
    async with httpx.AsyncClient() as client:
        while True:
            with _state_lock:
                cfg = _config.copy()
                targets = [(pid, _proxies[pid]["url"]) for pid in sorted(_proxies.keys())]

            interval = cfg["check_interval_seconds"]
            timeout_s = cfg["request_timeout_ms"] / 1000.0

            for pid, url in targets:
                ok = await _probe_proxy(client, url, timeout_s)
                at = _utc_now_iso()
                with _state_lock:
                    if pid not in _proxies:
                        continue
                    rec = _proxies[pid]
                    if ok:
                        rec["status"] = "up"
                        rec["consecutive_failures"] = 0
                    else:
                        rec["consecutive_failures"] = int(rec.get("consecutive_failures") or 0) + 1
                        rec["status"] = "down"
                    rec["last_checked_at"] = at

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
    global _main_loop, _heartbeat_wake
    _main_loop = asyncio.get_running_loop()
    _heartbeat_wake = asyncio.Event()
    task = asyncio.create_task(_heartbeat_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
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
