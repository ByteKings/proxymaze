"""ProxyMaze — minimal FastAPI backend (in-memory state)."""

from __future__ import annotations

import os
from typing import Any, Literal
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator

app = FastAPI(title="ProxyMaze")

# --- In-memory state ---

_config: dict[str, int] = {
    "check_interval_seconds": 60,
    "request_timeout_ms": 5000,
}

_proxies: dict[str, dict[str, Any]] = {}


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


def _merge_config(update: ConfigUpdate) -> dict[str, int]:
    data = _config.copy()
    if update.check_interval_seconds is not None:
        data["check_interval_seconds"] = update.check_interval_seconds
    if update.request_timeout_ms is not None:
        data["request_timeout_ms"] = update.request_timeout_ms
    return data


# --- Routes ---


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()


@app.post("/config", response_model=ConfigResponse)
def post_config(body: ConfigUpdate) -> ConfigResponse:
    global _config
    _config = _merge_config(body)
    return ConfigResponse(**_config)


@app.get("/config", response_model=ConfigResponse)
def get_config() -> ConfigResponse:
    return ConfigResponse(**_config)


@app.post("/proxies", response_model=ProxiesUpsertResponse)
def post_proxies(body: ProxiesUpsert) -> ProxiesUpsertResponse:
    global _proxies
    if body.replace:
        _proxies = {}

    accepted = 0
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

    if errors and accepted == 0 and body.proxies:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": "No valid proxies in request", "errors": errors},
        )

    records = [ProxyRecord(**_proxies[k]) for k in sorted(_proxies.keys())]
    return ProxiesUpsertResponse(accepted=accepted, proxies=records)


@app.get("/proxies", response_model=ProxiesListResponse)
def get_proxies() -> ProxiesListResponse:
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
