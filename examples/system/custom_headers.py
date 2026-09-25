#!/usr/bin/env python3
"""Send custom request headers, configured per client.

Headers live on ``ClientConfig`` (or ``DataQuery`` kwargs) and are never read
from the environment, so each client in one process can identify itself
differently. They go on every DataQuery API request: JSON, file and SSE.

Run:
  python examples/system/custom_headers.py
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))  # noqa: E402

from dataquery import ConfigurationError, DataQuery, EnvConfig  # noqa: E402

EnvConfig.load_env_file(ROOT / ".env")


async def main():
    # Credentials (and everything else) come from .env / the environment as usual.
    base = EnvConfig.create_client_config()

    # 1. Headers on a ClientConfig. Give each client its own config: two
    #    services in one process, each identifying itself differently.
    risk_config = base.model_copy(
        update={
            "custom_headers": {
                "X-User-Agent": "RiskEngine/2.1",
                "X-Team": "rates",
                "X-Request-Source": "nightly-batch",
            }
        }
    )
    ui_config = base.model_copy(update={"custom_headers": {"X-User-Agent": "ReportingUI/1.0", "X-Team": "fx"}})
    print("risk client sends:", risk_config.get_custom_headers())
    print("ui client sends:  ", ui_config.get_custom_headers())

    async with DataQuery(risk_config) as risk, DataQuery(ui_config) as ui:
        risk_groups, ui_groups = await asyncio.gather(
            risk.list_groups_async(limit=5),
            ui.list_groups_async(limit=5),
        )
        print(f"risk client listed {len(risk_groups)} groups, ui client listed {len(ui_groups)}")

    # 2. The same thing as kwargs, on top of env/.env resolution.
    async with DataQuery(custom_headers={"X-User-Agent": "Notebook/0.1", "X-Team": "research"}) as dq:
        print("notebook client sends:", dq.client_config.get_custom_headers())
        groups = await dq.list_groups_async(limit=5)
        print(f"notebook client listed {len(groups)} groups")

    # 3. Invalid headers fail when the client is created, before any request,
    #    and the error never shows the header value. Authorization always comes
    #    from your credentials, so it can't be set here.
    for bad in ({"Authorization": "Bearer abc"}, {"X-Api-Key": "s3cret\n"}, {"Bad Name": "x"}):
        try:
            DataQuery(custom_headers=bad)
        except ConfigurationError as exc:
            print("rejected:", exc)


if __name__ == "__main__":
    asyncio.run(main())
