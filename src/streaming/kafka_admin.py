"""src/streaming/admin.py - Kafka topic management example.

Creates, inspects, and optionally deletes Kafka topics.

Author: Denise Case
Date: 2026-05

Run this BEFORE the producer and consumer to ensure the topic exists.
Run this AFTER to delete the topic and start fresh.

Terminal commands to run from the root project folder:

  Create the topic (safe to run repeatedly):
    uv run python -m streaming.admin

  Delete the topic and recreate it (start fresh):
    uv run python -m streaming.admin --recreate

  Delete the topic only:
    uv run python -m streaming.admin --delete

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it admin_yourname.py, and modify your copy.

NOTE ON KAFKA STARTUP NOISE:
  You may see lines like:
    FAIL [rdkafka#...] Connect to ipv4#127.0.0.1:9092 failed: Unknown error
  These appear while rdkafka completes its internal broker handshake (~2 seconds).
  They are not errors in your code. If topic operations succeed, everything is working.
"""

# === DECLARE IMPORTS ===

import argparse
import os
from typing import Final

from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    create_topic,
    delete_topic,
    list_topics,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header
from dotenv import load_dotenv

# === CONFIGURE LOGGER ===

LOG = get_logger("A02", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

# Read environment variables from the .env file into os.environ.
load_dotenv()

# === DECLARE GLOBAL CONSTANTS ===

# These constants define our simulated online sales stream.
# In a real system, this data would come from a live website or payment API.
MESSAGE_COUNT: Final[int] = int(os.getenv("MESSAGE_COUNT", "6"))
MESSAGE_INTERVAL_SECONDS: Final[float] = float(
    os.getenv("MESSAGE_INTERVAL_SECONDS", "1.0")
)


# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Main entry point for Kafka topic management."""
    log_header(LOG, "A02")

    # === PARSE COMMAND-LINE ARGUMENTS ===

    parser = argparse.ArgumentParser(
        description="Kafka topic management for streaming examples."
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the topic. All messages will be removed.",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Delete the topic if it exists, then recreate it fresh.",
    )
    args = parser.parse_args()

    LOG.info("========================")
    LOG.info("START admin main()")
    LOG.info("========================")

    settings = KafkaSettings.from_env()

    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC             = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID          = {settings.group_id}")
    LOG.info(f"KAFKA_AUTO_OFFSET_RESET = {settings.auto_offset_reset}")

    LOG.info(f"MESSAGE_INTERVAL_SECONDS = {MESSAGE_INTERVAL_SECONDS}")
    LOG.info(f"MESSAGE_COUNT           = {MESSAGE_COUNT}")

    try:
        LOG.info("Verifying Kafka connection...")
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error

    admin = create_admin_client(settings)

    # --- LIST ALL TOPICS BEFORE ANY CHANGES ---

    existing = list_topics(admin)
    LOG.info(f"Topics currently in Kafka: {existing if existing else '(none)'}")

    # --- DELETE ---

    if args.delete or args.recreate:
        if topic_exists(admin, settings.topic):
            LOG.info(f"Deleting topic {settings.topic!r} ...")
            try:
                delete_topic(admin, settings.topic)
                LOG.info(f"Topic {settings.topic!r} deleted. All messages removed.")
            except RuntimeError as error:
                LOG.error(str(error))
                raise SystemExit(1) from error
        else:
            LOG.info(f"Topic {settings.topic!r} does not exist. Nothing to delete.")

    if args.delete and not args.recreate:
        existing = list_topics(admin)
        LOG.info(f"Topics currently in Kafka: {existing if existing else '(none)'}")
        LOG.info("========================")
        LOG.info("Admin executed successfully!")
        LOG.info("========================")
        return

    # --- CREATE (default behavior, or after --recreate) ---

    if topic_exists(admin, settings.topic):
        LOG.info(f"Topic {settings.topic!r} already exists. No action needed.")
        LOG.info("To start fresh, run:  uv run python -m streaming.admin --recreate")
    else:
        LOG.info(f"Creating topic {settings.topic!r} ...")
        try:
            create_topic(admin, settings.topic)
            LOG.info(f"Topic {settings.topic!r} created successfully.")
        except RuntimeError as error:
            LOG.error(str(error))
            raise SystemExit(1) from error

    # --- LIST ALL TOPICS AFTER CHANGES ---

    existing = list_topics(admin)
    LOG.info(f"Topics currently in Kafka: {existing if existing else '(none)'}")

    LOG.info("========================")
    LOG.info("Admin executed successfully!")
    LOG.info("========================")


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
