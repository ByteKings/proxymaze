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

Interactive docs: [http://localhost:8000/docs](http://localhost:8000/docs) (Swagger UI).

## API overview

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check (`{"status":"ok"}`). |
| `GET` | `/config` | Current settings: `check_interval_seconds`, `request_timeout_ms`. |
| `POST` | `/config` | Partial update; omit fields you do not want to change. Values must be ≥ 1 when set. |
| `POST` | `/proxies` | Register proxy URLs. Optional `replace: true` clears existing entries first. |
| `GET` | `/proxies` | List all proxies plus aggregates: `total`, `up`, `down`, `failure_rate`. |

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
- Registered proxies start as `pending`; there is no background health checker wired up in this minimal version—the config keys are placeholders for a future checker.
