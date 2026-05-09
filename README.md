# ProxyMaze

Minimal [FastAPI](https://fastapi.tiangolo.com/) API for managing proxy configuration and an in-memory proxy registry. State lives only in process memory and resets when the server stops.

## Requirements

- Python 3.10+ (uses `list[str]` union syntax as in the codebase)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Or:

```bash
python main.py
```

`python main.py` reads the port from the `PORT` environment variable (default `8000`), which matches platforms like Render that inject `PORT`.

Interactive docs (local): [http://localhost:8000/docs](http://localhost:8000/docs) (Swagger UI).
Interactive docs (Render): [https://proxymaze-gvpj.onrender.com/docs#/](https://proxymaze-gvpj.onrender.com/docs#/).

## API overview

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check (`{"status":"ok"}`). |
| `GET` | `/config` | Current settings: `check_interval_seconds`, `request_timeout_ms`. |
| `POST` | `/config` | Heartbeat settings: **`check_interval_seconds`** (pause between full passes) and **`request_timeout_ms`** (per-probe HTTP timeout). Partial update OK. **200 OK** returns the merged config and new values apply **immediately** (current sleep is interrupted so the next pass runs under the new rules). Defaults: `15` / `3000`. Example: `{"check_interval_seconds":15,"request_timeout_ms":3000}`. |
| `POST` | `/proxies` | Load proxies into the pool. **201 Created**. `replace: true` clears current pool first; omitted/false appends. Unknown extra fields are ignored. |
| `GET` | `/proxies` | List all proxies plus aggregates: `total`, `up`, `down`, `failure_rate`. |
| `DELETE` | `/proxies` | Clear the current proxy pool. **204 No Content**. |
| `GET` | `/alerts` | Return all alerts (active + resolved) as a JSON array. |
| `POST` | `/webhooks` | Register raw JSON alert event receiver. **201 Created**. |
| `POST` | `/integrations` | Register Slack/Discord formatted alert integration. **201 Created**. |
| `GET` | `/metrics` | Return operational monitoring metrics. **200 OK**. |

### Chapter 04: POST /proxies (Building the Pool)

Request example:

```json
{
  "proxies": [
    "https://proxy-provider.example/proxy/px-101",
    "https://proxy-provider.example/proxy/px-102"
  ],
  "replace": true
}
```

Rules:

- `replace` omitted or `false`: append to current pool.
- `replace: true`: clear pool first, then load provided proxies.
- New proxies start as `pending` and transition to `up`/`down` via background probes.
- Unknown request fields are ignored cleanly.

Response is **201 Created** with accepted count and accepted proxies from that request.

### Chapter 05: GET /proxies (The Watchtower)

Returns a live pool summary and per-proxy state with **200 OK**:

```json
{
  "total": 10,
  "up": 7,
  "down": 3,
  "failure_rate": 0.3,
  "proxies": [
    {
      "id": "px-101",
      "url": "https://proxy-provider.example/proxy/px-101",
      "status": "up",
      "last_checked_at": "2026-04-24T10:15:30Z",
      "consecutive_failures": 0
    }
  ]
}
```

Each proxy includes at minimum `id`, `url`, `status`, `last_checked_at`, and `consecutive_failures`.
Values reflect the latest **background heartbeat** result; `GET /proxies` does not trigger a fresh probe.

### Chapter 08: DELETE /proxies (The Graveyard)

`DELETE /proxies` clears the active pool and returns **204 No Content**.

- After purge, `GET /proxies` returns an empty pool.
- Alert history is preserved.
- `GET /alerts` remains accessible after purge.

### Chapter 09: GET /alerts (The Alert Archive)

Returns all alerts, both active and resolved, with **200 OK** as a JSON array:

```json
[
  {
    "alert_id": "alert-a1b2c3",
    "status": "active",
    "failure_rate": 0.3,
    "total_proxies": 10,
    "failed_proxies": 3,
    "failed_proxy_ids": ["px-103", "px-104", "px-105"],
    "threshold": 0.2,
    "fired_at": "2026-04-24T10:20:00Z",
    "resolved_at": null,
    "message": "Proxy pool failure rate exceeded threshold"
  }
]
```

Required alert fields:

- `alert_id`: non-empty and stable for the lifetime of that alert.
- `status`: `active` while breach holds; `resolved` after recovery.
- `failure_rate`: failure rate that justified/maintains the alert (`>= 0.2` while active).
- `total_proxies`: pool size at fire time.
- `failed_proxies`: count currently down.
- `failed_proxy_ids`: IDs currently down.
- `threshold`: fixed at `0.2`.
- `fired_at`: ISO 8601 UTC timestamp when breach began.
- `resolved_at`: ISO 8601 UTC timestamp on recovery, otherwise `null`.
- `message`: short human-readable summary.

Lifecycle rules:

- At most one alert is **active** at any time.
- While breach persists, the same active `alert_id` remains active (no duplicate active alerts).
- Once resolved, that alert remains in history unchanged except resolution fields.
- A later fresh breach creates a **new** `alert_id`.

### Chapter 10: POST /webhooks (The Messenger)

Registers a URL to receive raw alert lifecycle webhooks.

Request:

```json
{
  "url": "https://receiver.example/proxywatch-webhook"
}
```

Response (**201 Created**):

```json
{
  "webhook_id": "wh-123",
  "url": "https://receiver.example/proxywatch-webhook"
}
```

Raw lifecycle payloads:

- `alert.fired`:

```json
{
  "event": "alert.fired",
  "alert_id": "alert-a1b2c3",
  "fired_at": "2026-04-24T10:20:00Z",
  "failure_rate": 0.3,
  "total_proxies": 10,
  "failed_proxies": 3,
  "failed_proxy_ids": ["px-103", "px-104", "px-105"],
  "threshold": 0.2,
  "message": "Proxy pool failure rate exceeded threshold"
}
```

- `alert.resolved`:

```json
{
  "event": "alert.resolved",
  "alert_id": "alert-a1b2c3",
  "resolved_at": "2026-04-24T10:30:00Z"
}
```

Delivery behavior:

- `Content-Type: application/json` is used.
- Each state transition is delivered to each registered receiver.
- Transient errors (`500`, `502`, `503`, `504`) and network errors are retried until success.
- Duplicate successful deliveries are prevented per transition/receiver pair.

### Chapter 11: POST /integrations (The Integration Layer)

Registers Slack or Discord formatted alert integrations.

Slack request:

```json
{
  "type": "slack",
  "webhook_url": "https://receiver.example/slack",
  "username": "ProxyWatch",
  "events": ["alert.fired", "alert.resolved"]
}
```

Discord request:

```json
{
  "type": "discord",
  "webhook_url": "https://receiver.example/discord",
  "username": "ProxyWatch",
  "events": ["alert.fired", "alert.resolved"]
}
```

Response (**201 Created**):

```json
{
  "integration_id": "int-1",
  "type": "slack",
  "webhook_url": "https://receiver.example/slack",
  "username": "ProxyWatch",
  "events": ["alert.fired", "alert.resolved"]
}
```

Notes:

- Additional request fields are accepted and ignored.
- `events` controls which lifecycle events the integration receives.
- Slack payloads are sent as `{ \"username\": \"...\", \"text\": \"...\" }`.
- Discord payloads are sent as `{ \"username\": \"...\", \"content\": \"...\" }`.

### Chapter 12: GET /metrics (The Control Room)

Returns operational monitoring counters with **200 OK**:

```json
{
  "total_checks": 120,
  "current_pool_size": 10,
  "active_alerts": 1,
  "total_alerts": 3,
  "webhook_deliveries": 4
}
```

Field meaning:

- `total_checks`: cumulative number of proxy checks completed by the background heartbeat.
- `current_pool_size`: number of proxies currently loaded in the pool.
- `active_alerts`: current count of active alerts (expected to be `0` or `1` with lifecycle rules).
- `total_alerts`: total alert objects recorded in history (active + resolved).
- `webhook_deliveries`: cumulative successful outbound deliveries (webhooks + integrations).

### Proxy URLs

Each proxy string must be an absolute `http` or `https` URL. The **last path segment** is used as the proxy id (for example, `https://proxy.example.com/path/px-101` → id `px-101`). URLs without a non-empty final segment are rejected.

### Example

```bash
curl -s http://localhost:8000/health
curl -s -X POST http://localhost:8000/proxies \
  -H "Content-Type: application/json" \
  -d '{"proxies":["https://example.com/p/a/proxy-1"],"replace":false}'
curl -s http://localhost:8000/proxies
```

## Limitations

- No persistence: restarting the process clears config overrides and the proxy list.
- The heartbeat probes each proxy URL with **HEAD**, then **GET** if HEAD fails or returns 405. Status is `up` if the response is successful (typical 2xx/3xx); connection errors and most failures mark `down`.
