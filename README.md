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
| `GET` | `/alerts` | Return alert history (currently an in-memory list). |

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
