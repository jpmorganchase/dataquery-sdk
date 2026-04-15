#!/usr/bin/env python3
"""
SSE local-server example — group-scoped subscriptions.

Connects the DataQuery SDK's SSE client to the local Spring Boot
dataquery-sse-server (http://localhost:8080).

Each user may only subscribe to groups they are allowed to access.
Attempting to subscribe to a forbidden group returns HTTP 403.

User/group mapping (defined in application.properties):
    alice  (token: alice-token-123)  →  group-a, group-b
    bob    (token: bob-token-456)    →  group-b, group-c

Run the Spring Boot server first:
    cd dataquery-sse-server && mvn spring-boot:run

Trigger a notification from another terminal:
    # alice triggers group-a (allowed)
    curl -X POST http://localhost:8080/groups/group-a/trigger \\
         -H "Content-Type: application/json" \\
         -H "Authorization: Bearer alice-token-123" \\
         -d '{"fileDateTime":"20240305"}'

    # bob tries to trigger group-a (403 Forbidden)
    curl -X POST http://localhost:8080/groups/group-a/trigger \\
         -H "Content-Type: application/json" \\
         -H "Authorization: Bearer bob-token-456" \\
         -d '{"fileDateTime":"20240305"}'

Press Ctrl+C to stop.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import SSEClient, SSEEvent  # noqa: E402
from dataquery.auth import OAuthManager, TokenManager  # noqa: E402
from dataquery.models import ClientConfig  # noqa: E402

SERVER_BASE = "http://localhost:8080"

# Users defined in application.properties
USERS = {
    "alice": {"token": "alice-token-123", "groups": ["group-a", "group-b"]},
    "bob": {"token": "bob-token-456", "groups": ["group-b", "group-c"]},
}


def build_config(bearer_token: str, group_id: str) -> ClientConfig:
    """
    ClientConfig for a specific user+group pair.

    The SSEClient builds the notification URL as:
        {api_base_url}/notification
    Setting base_url to "http://localhost:8080/groups/<groupId>" and
    context_path=None means:
        api_base_url = "http://localhost:8080/groups/<groupId>"
        notification URL = "http://localhost:8080/groups/<groupId>/notification"
    """
    return ClientConfig(
        base_url=f"{SERVER_BASE}/groups/{group_id}",
        context_path=None,
        oauth_enabled=False,
        bearer_token=bearer_token,
        files_base_url=None,
    )


async def run_sse_client(user: str, group_id: str):
    """Connect as *user* to *group_id* and print incoming events."""
    info = USERS[user]
    token = info["token"]
    allowed = info["groups"]

    print(f"\n[{user}] Connecting to group '{group_id}'")
    if group_id not in allowed:
        print(f"[{user}] WARNING: '{group_id}' is NOT in this user's allowed groups {allowed}")
        print(f"[{user}] Expect HTTP 403 from the server.\n")
    else:
        print(f"[{user}] Allowed groups: {allowed}\n")

    config = build_config(token, group_id)
    auth = TokenManager(config)
    oauth_manager = OAuthManager(config, auth)

    def on_event(event: SSEEvent):
        print(f"[{user}@{group_id}] EVENT  id={event.id}  type={event.event!r}")
        print(f"[{user}@{group_id}]        data={event.data}\n")

    def on_error(exc: Exception):
        print(f"[{user}@{group_id}] ERROR: {exc}\n")

    client = SSEClient(
        config=config,
        auth_manager=oauth_manager,
        on_event=on_event,
        on_error=on_error,
        reconnect_delay=3.0,
    )

    await client.start()
    print(f"[{user}@{group_id}] Listening… (Ctrl+C to stop)\n")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{user}@{group_id}] Stopping...")
    finally:
        await client.stop()


# ── Scenario menu ─────────────────────────────────────────────────────────────

SCENARIOS = {
    "1": ("alice subscribes to group-a  (allowed)", "alice", "group-a"),
    "2": ("alice subscribes to group-b  (allowed)", "alice", "group-b"),
    "3": ("bob   subscribes to group-b  (allowed)", "bob", "group-b"),
    "4": ("bob   subscribes to group-a  (403 expected)", "bob", "group-a"),
    "5": ("alice subscribes to group-c  (403 expected)", "alice", "group-c"),
}


async def main():
    print("DataQuery SDK — Group-scoped SSE Example")
    print("=" * 50)
    print(f"Server: {SERVER_BASE}\n")
    print("User/group access-control (from application.properties):")
    for user, info in USERS.items():
        print(f"  {user:6s}  token={info['token']:<20s}  groups={info['groups']}")
    print()
    for key, (label, _, _) in SCENARIOS.items():
        print(f"  {key}. {label}")
    choice = input("\nSelect scenario [1]: ").strip() or "1"

    _, user, group_id = SCENARIOS.get(choice, SCENARIOS["1"])
    await run_sse_client(user, group_id)


if __name__ == "__main__":
    asyncio.run(main())
