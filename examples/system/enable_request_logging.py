"""
Example: Enable request/response logging via LoggingManager.

Run:
  python -m examples.system.enable_request_logging --env-file .env
"""

import asyncio
from pathlib import Path
from typing import Optional

from dataquery.dataquery import DataQuery
from dataquery.logging_config import (
    LogFormat,
    LogLevel,
    create_logging_config,
    create_logging_manager,
)


async def main(env_file: Optional[Path] = None):
    # Configure console logging and turn on request logging
    logging_config = create_logging_config(
        level=LogLevel.DEBUG,
        format=LogFormat.CONSOLE,
        enable_console=True,
        enable_file=False,
        enable_request_logging=True,  # key line to enable HTTP request/response logs
        enable_performance_logging=True,
    )
    manager = create_logging_manager(logging_config)
    logger = manager.get_logger("example")
    logger.info("Starting example with request logging enabled")

    # Use DataQuery as usual
    async with DataQuery(env_file) as dq:
        healthy = await dq.health_check_async()
        logger.info("Health check", healthy=healthy)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=None)
    args = parser.parse_args()

    asyncio.run(main(args.env_file))
