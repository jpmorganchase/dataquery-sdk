# Auto-Download: SSE-driven 24/7 file retrieval

`DataQuery.auto_download_async(...)` is the SDK's push-based watcher. It holds a
persistent Server-Sent Events (SSE) connection to the DataQuery notification
endpoint and downloads each new file as soon as it is announced. This document
explains the moving parts, the retry strategy at every layer, and how to run
the manager reliably for days or weeks at a time.

---

## 1. Components

| Component | File | Responsibility |
|---|---|---|
| `DataQuery.auto_download_async` | `dataquery.py` | Public facade; delegates to the client. |
| `DataQueryClient.auto_download_async` | `core/client.py` | Constructs a `NotificationDownloadManager` and calls `start()`. |
| `NotificationDownloadManager` | `sse/subscriber.py` | Subscribes to events, dedups, triggers downloads, tracks stats. |
| `SSEClient` | `sse/client.py` | Holds the HTTP stream, parses SSE frames, reconnects on failure. |
| `SSEEventIdStore` | `sse/event_store.py` | Persists the most recently seen event id to disk for replay. |
| `download_and_track` | `download/utils.py` | Performs one download and updates stats / retry counters. |
| `TokenManager` / `OAuthManager` | `transport/auth.py` | Refreshes OAuth tokens in the background for long-running processes. |

---

## 2. High-level architecture

```
                    ┌──────────────────────────────────────────┐
                    │            User / CLI / service          │
                    └───────────────────┬──────────────────────┘
                                        │ await dq.auto_download_async(...)
                                        ▼
             ┌────────────────────────────────────────────────────────┐
             │            NotificationDownloadManager                 │
             │  • dedup set (_downloaded_files, LRU-bounded)          │
             │  • retry counter (_failed_files, LRU-bounded)          │
             │  • stats dict (counters + error ring buffer)           │
             │  • on_event → _handle_notification                     │
             └──────┬───────────────────────────────────┬─────────────┘
                    │ on_event                          │ download_and_track
                    ▼                                   ▼
       ┌─────────────────────────┐          ┌──────────────────────────┐
       │       SSEClient         │          │   DataQueryClient        │
       │  • persistent GET on    │          │  (rate-limited,          │
       │    /sse/event/...       │          │   OAuth-refreshing,      │
       │  • reconnect w/ backoff │          │   circuit-broken)        │
       │  • heartbeat watchdog   │          │                          │
       │  • Last-Event-ID replay │          │  download_file_async     │
       └──────────┬──────────────┘          │    → ranged parallel GET │
                  │                         └──────────────────────────┘
                  ▼                                      ▲
       ┌─────────────────────────┐                       │
       │   SSEEventIdStore       │                       │ HTTP(S)
       │  ~/.dataquery/state/    │                       │
       │  sse_<fingerprint>.json │                       ▼
       └─────────────────────────┘          ┌──────────────────────────┐
                                            │    DataQuery REST API    │
                                            │  /sse/event/notification │
                                            │  /group/*  /file/*       │
                                            └──────────────────────────┘
```

---

## 3. Event lifecycle (one notification → one file on disk)

```
Server publishes event
        │
        ▼
SSEClient reads bytes from the open stream
        │  parses  data:, event:, id:, retry: fields
        ▼
SSEEventIdStore.save(event.id)   ◀──── async persist for crash recovery
        │
        ▼
NotificationDownloadManager._on_sse_event(event)
        │
        ▼
_handle_notification(event)
        │
        ├── file_key already in _downloaded_files?      → skip (dedup)
        ├── _failed_files[file_key] ≥ max_retries?      → skip (retry cap)
        ├── file already on disk?                        → mark done, skip
        ├── file_filter(...) is False?                   → skip (user predicate)
        │
        ▼
client.check_availability_async(file_group_id, file_datetime)
        │  not is_available → return, wait for a later event
        │
        ▼
download_and_track(...)
        │
        │  try: client.download_file_async(...)                      (ranged, parallel)
        │      result.status == COMPLETED      → downloaded_files.add(key),
        │                                         failed_files.pop(key)
        │      result.status == ALREADY_EXISTS → treat as success
        │      else                            → failed_files[key] += 1
        │  except Exception:                    → failed_files[key] += 1,  raise
        ▼
stats updated:
  notifications_received, checks_triggered, files_discovered,
  files_downloaded, files_skipped, download_failures, total_bytes_downloaded
```

---

## 4. Retry handling — three independent layers

The SDK deliberately separates *what* is being retried. Each layer has its own
budget and backoff so a failure in one does not block the others.

### 4.1 HTTP request retries (inside every API call)

Handled by `transport/retry.py` (`RetryManager` + `CircuitBreaker`).

- Applies to `check_availability_async`, `download_file_async`, token refresh,
  and every other REST call made by the SSE manager.
- **3 attempts** by default, **exponential backoff** between them.
- Circuit breaker opens after sustained failures; `AuthenticationError`,
  `RateLimitError`, `NetworkError`, `DownloadError` all get different
  decisions.
- Invisible to `NotificationDownloadManager`: by the time it sees a failure,
  the low-level retry budget has already been spent.

### 4.2 SSE connection retries (keeps the subscription alive)

Handled by `SSEClient._run_loop` (`sse/client.py:180`).

```
delay = reconnect_delay          # default 5.0s
while running:
    try: _connect_and_listen()   # blocks until stream ends / errors
    except CancelledError: break
    except Exception as e: on_error(e)

    wait(delay, or until stop())

    if last_connection_duration ≥ SSE_HEALTHY_CONNECTION_SECONDS:
        delay = reconnect_delay  # healthy session → reset to 5s
    else:
        delay = min(delay * 2, max_reconnect_delay)  # backoff up to 60s
```

Key properties:

- **Infinite retries.** There is no "give up" for the SSE stream — the outer
  loop runs until `stop()` is called.
- **Backoff resets on a healthy session.** A server that recycles the stream
  every 5 minutes as an idle timeout does not ramp up to the cap — the backoff
  goes back to 5s each time.
- **Event replay on reconnect.** The `Last-Event-ID` header is sent on every
  reconnect, and the `last-event-id` query parameter carries the persisted id
  on the very first connection of a new process. The server fills in events
  published while we were disconnected.
- **Heartbeat watchdog.** When `heartbeat_timeout > 0`, a background task
  forces a reconnect if no bytes (events *or* SSE comment keep-alives) arrive
  within the window. Catches half-open TCP / stalled-proxy hangs that the
  server's own close path would never trigger.

### 4.3 Per-file download retries (caps work on broken files)

Handled by `NotificationDownloadManager._handle_notification` + `download_and_track`.

- `_failed_files[file_key]` is **incremented** on every failed download.
- It is **cleared** on success or `ALREADY_EXISTS`.
- Before each attempt, if the counter has reached `max_retries` (default 3),
  the file is skipped permanently — further events for that key are ignored.
- **There is no automatic re-trigger.** The manager does not schedule its own
  retries. A previously-failed file is retried only when:
  1. The server sends another SSE event for that file, or
  2. The next process start runs `initial_check` and finds it available.
  
  This is intentional: transient failures (network blips) tend to clear on
  the next notification anyway, while genuinely broken files stop consuming
  retries after the cap.

Both tracking structures (`_downloaded_files`, `_failed_files`) are
`_BoundedRetryMap` / `_BoundedKeySet` LRU containers sized by
`max_tracked_files` (default 10,000). Keys still seeing traffic stay hot;
cold keys fall out. Memory cannot grow unboundedly regardless of uptime.

---

## 5. Crash recovery and startup flow

```
Process starts
    │
    ├── DataQuery(...)            # load config from env / .env
    │   └── ClientConfig          # rate-limit, retries, base URL, OAuth
    │
    ▼
auto_download_async(group_id, file_group_id=…)
    │
    ▼
NotificationDownloadManager.start()
    │
    ▼
enable_event_replay?
    ├── yes → SSEEventIdStore.load()    reads ~/.dataquery/state/sse_<fp>.json
    │        │
    │        ├── found id → SKIP initial_check; pass id on first SSE connect
    │        │              (server replays every event since that id)
    │        │
    │        └── no id   → run initial_check (one-shot bulk availability
    │                        query for today's date), then connect
    │
    └── no  → run initial_check if initial_check=True, then connect
    │
    ▼
SSEClient.start()  → enters _run_loop forever until stop()
```

On graceful shutdown (`await mgr.stop()`):

1. `_running = False` — blocks new event handling.
2. `SSEClient.stop()` — closes the open HTTP connection, cancels the run loop.
3. In-flight downloads finish naturally (the manager awaits them).
4. The last persisted event id stays on disk, ready for the next process.

On a crash (OOM, SIGKILL, host reboot):

- Whatever `event.id` was most recently observed has been written to disk by
  `SSEEventIdStore.save(...)` using an atomic `write-temp + os.replace` dance.
- The next process reads it in `load()` and the server replays every event
  since — there is no gap.
- Files that were being downloaded when the process died are simply
  rediscovered via either replay or the next event for the same file.
  Partial files on disk are overwritten or resumed per `DownloadOptions`.

---

## 6. Running 24/7

The SDK guarantees the *subscription* is durable — but the OS has to keep the
*process* alive. Use a supervisor.

### 6.1 Recommended configuration

```python
import asyncio
from dataquery import DataQuery

async def main() -> None:
    async with DataQuery() as dq:          # credentials from env / .env
        mgr = await dq.auto_download_async(
            group_id="economic-data",
            destination_dir="/data/dataquery",
            max_retries=3,                 # per-file cap
            max_concurrent_downloads=5,
            enable_event_replay=True,      # disk-persisted last-event-id
            heartbeat_timeout=120.0,       # forces reconnect on silent stream
            reconnect_delay=5.0,
            max_reconnect_delay=60.0,
            max_tracked_files=10_000,      # LRU bound on dedup / retry maps
        )
        # Block until SIGINT/SIGTERM or mgr.stop() from elsewhere
        try:
            while mgr.is_running:
                await asyncio.sleep(60)
        finally:
            await mgr.stop()

asyncio.run(main())
```

### 6.2 Process supervision

The SDK reconnects TCP streams forever — it does not restart Python on a
segfault, OOM kill, or `systemctl restart`. Pick one:

**systemd** (Linux hosts):

```ini
# /etc/systemd/system/dataquery-watch.service
[Unit]
Description=DataQuery SSE watcher
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=dataquery
EnvironmentFile=/etc/dataquery/env
WorkingDirectory=/opt/dataquery
ExecStart=/opt/dataquery/.venv/bin/python -m dataquery.run_watch
Restart=always
RestartSec=10
# Critical: send SIGTERM, then wait before SIGKILL so mgr.stop() runs.
KillSignal=SIGTERM
TimeoutStopSec=30

[Install] 
WantedBy=multi-user.target
```

**Docker / Kubernetes**:

```yaml
# docker-compose.yml
services:
  dataquery-watch:
    image: myorg/dataquery-watch:latest
    restart: unless-stopped
    env_file: ./dataquery.env
    volumes:
      - dataquery-state:/root/.dataquery   # persists sse_<fp>.json
      - dataquery-downloads:/data
```

```yaml
# k8s deployment
spec:
  restartPolicy: Always            # (default for Deployment)
  template:
    spec:
      terminationGracePeriodSeconds: 30
      containers:
      - name: watch
        image: myorg/dataquery-watch:latest
        volumeMounts:
          - mountPath: /root/.dataquery
            name: state            # PVC so event id survives pod restart
```

**Important**: the SSE event-id file lives in the state dir returned by the
config (see `SSEEventIdStore` construction). Mount it on a persistent volume
so container restarts replay correctly.

### 6.3 Tuning checklist

| Knob | Default | When to change |
|---|---|---|
| `max_retries` | 3 | Raise to 5 for flaky networks; lower to 1 if transient failures are rare and you want to see them in the logs. |
| `max_concurrent_downloads` | 5 | Raise if the rate limiter is not the bottleneck; lower to avoid saturating downstream storage. |
| `reconnect_delay` | 5.0s | Lower to ~1s for latency-sensitive pipelines; raise if the API has tight connect budgets. |
| `max_reconnect_delay` | 60.0s | Raise to 300s to be kinder during regional outages. |
| `heartbeat_timeout` | 0 (off) | **Set it.** 90–180s detects half-open TCP sessions that the server's 5-minute recycle can't. |
| `initial_check` | `True` | Set `False` only when `enable_event_replay=True` and you trust the stored id. |
| `enable_event_replay` | `True` | Only disable if your subscription is idempotent and you don't care about short-window gaps. |
| `max_tracked_files` | 10,000 | Raise for extremely high-volume subscriptions; lower to reduce memory. |

### 6.4 Health monitoring

`mgr.get_stats()` returns a JSON-friendly snapshot for metrics scrapers:

```python
{
  "notifications_received": 1842,
  "checks_triggered": 1842,
  "files_discovered": 1830,
  "files_downloaded": 1825,
  "files_skipped": 5,
  "download_failures": 12,
  "total_bytes_downloaded": 48217318912,
  "runtime_seconds": 86412.0,
  "is_running": true,
  "last_event_id": "evt_0199abc...",
  "downloaded_file_keys": 1825,
  "failed_file_keys": 7,
  "errors": [ ... last 1000 ... ]
}
```

Good signals to alert on:

- `is_running == False` while the process is up (manager crashed, supervisor should have restarted by now).
- `download_failures / files_discovered` climbing over a window.
- `last_event_id` unchanged while `runtime_seconds` grows — the stream is live but the server is silent. Combined with traffic expectations, this catches both real outages and silent auth failures.
- OAuth errors in the `errors` ring buffer — rotate credentials.

---

## 7. What auto-download does *not* do

Being explicit about non-goals so operators don't expect them:

- It does not **poll** for files. The legacy `AutoDownloadManager` was removed
  in favour of the SSE push model; `auto_download_async(...)` is the only
  watch API.
- It does not **backfill arbitrary history**. Replay works from the last seen
  event id, which is bounded by what the server retains. Use
  `download_historical_async` for large backfills.
- It does not **restart the Python process**. Use systemd / Docker / k8s.
- It does not **retry a previously failed file on a timer**. A file has to be
  re-announced (SSE event or startup `initial_check`) before a new attempt is
  made. This is by design — see §4.3.

---

## 8. Quick reference

- Public entry: `DataQuery.auto_download_async` (async) / CLI `dataquery download --watch`.
- State file: `~/.dataquery/state/sse_<fingerprint>.json` (persists `last-event-id`).
- Reset state: `mgr.clear_event_id()` or `dataquery download --watch --fresh`.
- Logs: `structlog` at `INFO` for lifecycle, `DEBUG` for per-event detail.
- Source of truth for behaviour:
  - `dataquery/sse/client.py` — connection / replay / heartbeat / backoff.
  - `dataquery/sse/subscriber.py` — dedup, retry cap, stats.
  - `dataquery/sse/event_store.py` — on-disk event id persistence.
  - `dataquery/download/utils.py` — per-file download + retry counter update.
  - `dataquery/transport/retry.py` — HTTP-level retries and circuit breaker.
