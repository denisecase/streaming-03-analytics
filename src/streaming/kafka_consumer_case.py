"""src/streaming/kafka_consumer_case.py.

Kafka consumer: analytics

Reads sales messages from a Kafka topic and runs the full pipeline:
  - Validates each message against the data contract
  - Computes derived fields (subtotal, tax amount, total)

Start with main() at the bottom.
Work up to see how it all fits together.

Many functions are standard helpers
and should not need project-specific modifications.

Author: Denise Case
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_case

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it consumer_yourname.py, and modify your copy.
"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row, read_csv_as_lookup
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_engineering.derived_fields import enrich_message
from streaming.data_validation.data_contract_case import (
    CONSUMED_FIELDNAMES,
    SALES_REQUIRED_FIELDS,
    validate_required_fields,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("C03", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

COURSE_NAME: Final[str] = "Streaming Data"
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_sales.csv"

REGIONS_CSV: Final[Path] = DATA_DIR / "regions.csv"
PRODUCTS_CSV: Final[Path] = DATA_DIR / "products.csv"
CURRENCIES_CSV: Final[Path] = DATA_DIR / "currencies.csv"
DISCOUNT_CODES_CSV: Final[Path] = DATA_DIR / "discount_codes.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C03")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "REGIONS_CSV", REGIONS_CSV)
    log_path(LOG, "PRODUCTS_CSV", PRODUCTS_CSV)
    log_path(LOG, "CURRENCIES_CSV", CURRENCIES_CSV)
    log_path(LOG, "DISCOUNT_CODES_CSV", DISCOUNT_CODES_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")

    # Create an admin client to check if the topic exists and has messages.
    admin = create_admin_client(settings)

    topic_exists_already = topic_exists(admin, settings.topic)

    if not topic_exists_already:
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    LOG.info(f"Topic {settings.topic!r} exists.")

    # Call a function to get count of messages
    # on the topic before we start consuming
    message_count = get_topic_message_count(admin, settings.topic, settings)

    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        # Exit with a non-zero code to indicate an error.
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so this example reads all available messages.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)

    # call consumer.subscribe() with an on_assign callback
    # to reset offsets to the beginning
    # This ensures the example reads all available messages every time it runs.
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> RunningStats:
    """Initialize output directory, CSV, and stats.

    Returns:
        A RunningStats instance.
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()
    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    return RunningStats()


def load_reference_data() -> dict[str, float]:
    """Load reference data used for message enrichment.

    Returns:
        A dictionary mapping region_id to tax rate as a float.
    """
    LOG.info("Loading enrichment reference data...")
    region_lookup: dict[str, float] = {
        region_id: float(tax_rate_pct)
        for region_id, tax_rate_pct in read_csv_as_lookup(
            REGIONS_CSV,
            key_field="region_id",
            value_field="tax_rate_pct",
        ).items()
    }
    LOG.info(f"Found {len(region_lookup)} region tax rates.")
    return region_lookup


def process_message(
    row: dict[str, Any],
    *,
    region_lookup: dict[str, float],
    stats: RunningStats,
) -> dict[str, Any] | None:
    """Process one consumed message.

    Arguments after the asterisk must be passed as keyword arguments.

    Steps:
      - Validate required fields
      - Enrich with derived fields
      - Update running statistics

    Arguments:
        row: A raw consumed Kafka message row.
        region_lookup: Tax rates by region_id.
        stats: Running statistics accumulator.

    Returns:
        The enriched row, or None if validation failed.
    """
    # First, validate the message against the data contract.
    # If validation fails, return None to indicate the message should be rejected.
    errors = validate_required_fields(record=row, required_fields=SALES_REQUIRED_FIELDS)
    if errors:
        LOG.warning(f"Validation failed for order {row.get('order_id', '?')}")
        LOG.warning(f"errors={errors}")
        return None

    # Then, enrich the message with derived fields.
    enriched = enrich_message(row, region_lookup)
    LOG.info(f"subtotal={enriched['subtotal']}")
    LOG.info(f"tax={enriched['tax_amount']}")
    LOG.info(f"total={enriched['total']}")
    LOG.info(f"running_total={stats.total + enriched['total']:.2f}")

    # Update running statistics with the new total.
    stats.update(enriched["total"])
    return enriched


def consume_messages(
    consumer: Any,
    *,
    region_lookup: dict[str, float],
    stats: RunningStats,
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        consumer: An open Kafka consumer subscribed to the topic.
        region_lookup: Tax rates by region_id.
        stats: Running statistics accumulator.

    Returns:
        A tuple of (consumed_count, skipped_count).
    """
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        enriched = process_message(
            row,
            region_lookup=region_lookup,
            stats=stats,
        )

        if enriched is None:
            skipped_count += 1
            LOG.warning("MESSAGE REJECTED")
            LOG.warning(f"order={row.get('order_id', '?')}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        append_csv_row(
            path=OUTPUT_CSV,
            row={field: enriched.get(field, "") for field in CONSUMED_FIELDNAMES},
            fieldnames=CONSUMED_FIELDNAMES,
        )

        consumed_count += 1
        LOG.info("MESSAGE ACCEPTED")
        LOG.info(f"order={enriched['order_id']}")
        LOG.info(f"total=${enriched['total']:.2f}")
        LOG.info(f"consumed={consumed_count}")
        LOG.info("RUNNING STATS")
        LOG.info(f"total_sales=${stats.total:,.2f}")
        LOG.info(f"average=${stats.mean:,.2f}")
        LOG.info(f"min=${stats.minimum:,.2f}")
        LOG.info(f"max=${stats.maximum:,.2f}")

    return consumed_count, skipped_count


def save_artifacts() -> None:
    """Save output artifacts."""
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)

    if stats.count > 0:
        LOG.info(f"  Total sales:  ${stats.total:,.2f}")
        LOG.info(f"  Average sale: ${stats.mean:,.2f}")
        LOG.info(f"  Minimum sale: ${stats.minimum:,.2f}")
        LOG.info(f"  Maximum sale: ${stats.maximum:,.2f}")

    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    stats = initialize_output()
    region_lookup = load_reference_data()

    consumed_count = 0
    skipped_count = 0

    try:
        consumed_count, skipped_count = consume_messages(
            consumer,
            region_lookup=region_lookup,
            stats=stats,
        )
    finally:
        consumer.close()
        LOG.info("Kafka consumer closed.")

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, skipped_count, stats, settings)


# === CONDITIONAL EXECUTION GUARD ===

# WHY: If running this file as a script, then call main().
# This is standard Python "boilerplate".

if __name__ == "__main__":
    main()
