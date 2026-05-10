"""src/streaming/kafka_producer_case.py - Kafka producer example.

Reads sales from data/sales.csv,
validates them against the data contract,
writes rejected records to a local CSV file,
and sends valid records to a Kafka topic one message at a time.

Start with main() at the bottom.
Work up to see how it all fits together.

Many functions are standard helpers
and should not need project-specific modifications.

Author: Denise Case
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_producer_case

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it producer_yourname.py, and modify your copy.
"""

# === DECLARE IMPORTS ===

from collections.abc import Generator
import os
from pathlib import Path
import time
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.io.errors import missing_csv_field_message
from datafun_streaming.io.io_utils import (
    append_csv_row,
    format_message_for_log,
    read_csv_rows,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_validation.data_contract_case import (
    PRODUCTS_REQUIRED_FIELDS,
    REGIONS_REQUIRED_FIELDS,
    REJECTED_SALES_FIELDNAMES,
    validate_sale_record,
)
from streaming.data_validation.data_validation_case import (
    add_validation_errors,
    make_lookup_set,
    validate_reference_records,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("P03", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

# get from .env as strings with defaults
msg_count = os.getenv("PRODUCER_MESSAGE_COUNT", "6")
msg_interval_seconds = os.getenv("PRODUCER_MESSAGE_INTERVAL_SECONDS", "2.0")

# then convert to correct types for CONSTANTS
MESSAGE_COUNT: Final[int] = int(msg_count)
MESSAGE_INTERVAL_SECONDS: Final[float] = float(msg_interval_seconds)

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

SALES_CSV: Final[Path] = DATA_DIR / "sales.csv"
REGIONS_CSV: Final[Path] = DATA_DIR / "regions.csv"
PRODUCTS_CSV: Final[Path] = DATA_DIR / "products.csv"
REJECTED_SALES_CSV: Final[Path] = OUTPUT_DIR / "producer_rejected_sales.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "P03")
    LOG.info("========================")
    LOG.info("START producer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "SALES_CSV", SALES_CSV)
    log_path(LOG, "REGIONS_CSV", REGIONS_CSV)
    log_path(LOG, "PRODUCTS_CSV", PRODUCTS_CSV)
    log_path(LOG, "REJECTED_SALES_CSV", REJECTED_SALES_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS           = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC                       = {settings.topic}")
    LOG.info(f"PRODUCER_MESSAGE_COUNT            = {MESSAGE_COUNT}")
    LOG.info(f"PRODUCER_MESSAGE_INTERVAL_SECONDS = {MESSAGE_INTERVAL_SECONDS}")
    LOG.info(f"KAFKA_CLEAR_TOPIC_ON_START        = {settings.clear_topic_on_start}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Arguments:
        settings: KafkaSettings instance with connection info.

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


def load_reference_data() -> tuple[set[str], set[str]]:
    """Load and validate reference data.

    Right now we just load region and product reference data for validation,
    but you can add more reference data and validation rules as needed.

    Returns:
        A tuple of (valid_region_ids, valid_product_ids).

    Raises:
        SystemExit: If any reference file is missing or invalid.
    """
    LOG.info("Loading validation reference data...")

    # initialize an empty list to collect validation error messages (strings)
    errors: list[str] = []

    # Use the read_csv_rows function()
    # to read from reference tables in data/

    # -----------------------------------------------------
    # === LOAD REGION VALIDATION DATA (see regions.csv) ===
    # -----------------------------------------------------

    # For example, get a list of region records.
    # Each one is a dictionary of fieldname to value,
    # like {"region_id": "US-MO", "region_name": "Missouri"}.
    region_records: list[dict[str, str]] = read_csv_rows(REGIONS_CSV)

    # validate the region records against the data contract rules for regions.csv
    # get back an empty list if all records are valid,
    # or a list of error messages if any problems
    region_error_list = validate_reference_records(
        records=region_records,
        required_fields=REGIONS_REQUIRED_FIELDS,
        label="regions.csv",
    )

    # Extend the main errors list with any errors found in the region records.
    errors.extend(region_error_list)

    # -----------------------------------------------------
    # === LOAD PRODUCT VALIDATION DATA (see products.csv) ===
    # -----------------------------------------------------

    # Get a list of product records.
    # Each one is a dictionary of fieldname to value,
    # like {"product_id": "P001", "product_name": "Product 1"}.
    product_records: list[dict[str, str]] = read_csv_rows(PRODUCTS_CSV)
    product_record_error_list = validate_reference_records(
        records=product_records,
        required_fields=PRODUCTS_REQUIRED_FIELDS,
        label="products.csv",
    )
    errors.extend(product_record_error_list)

    # -----------------------------------------------------
    # What other reference tables are available?
    # -----------------------------------------------------
    # Consider expanding this to provide additional validation rules.

    # -----------------------------------------------------
    # AFTER LOADING ALL REFERENCE DATA, CHECK FOR ANY ERRORS
    # -----------------------------------------------------

    if errors:
        for error in errors:
            LOG.error(error)
        LOG.error("Reference data failed validation. Fix reference files first.")
        raise SystemExit(1)

    # -----------------------------------------------------
    # MAKE LOOKUP SETS OF VALID IDS FOR LATER VALIDATION OF SALES MESSAGES
    # -----------------------------------------------------

    valid_region_ids = make_lookup_set(region_records, "region_id")
    LOG.info(f"Found {len(valid_region_ids)} valid regions, ")

    valid_product_ids = make_lookup_set(product_records, "product_id")
    LOG.info(f"{len(valid_product_ids)} valid products.")

    # return the results as a tuple of two sets
    return valid_region_ids, valid_product_ids


# ===========================================================================
# DEFINE SECTION P. PRODUCE MESSAGES HELPERS
# ===========================================================================


def get_message_key(message: dict[str, Any]) -> str:
    """Return the Kafka message key for a sale record.

    We use region_id as the message key
    so all sales from the same region
    go to the same Kafka partition,
    keeping them in order.
    """
    try:
        return str(message["region_id"])
    except KeyError as error:
        msg = missing_csv_field_message(
            field="region_id",
            available_fields=list(message.keys()),
        )
        raise KeyError(msg) from error


def generate_messages(count: int) -> Generator[dict[str, str]]:
    """Generate a stream of sales from the input CSV file.

    A generator function uses yield instead of return.
    It produces one value at a time instead of computing everything at once.
    This is how we model data in motion, one event arriving at a time.

    Arguments:
        count: How many sales to generate.

    Yields:
        One sale row dictionary at a time.
    """
    sales_rows = read_csv_rows(SALES_CSV)
    yield from sales_rows[:count]


def write_rejected_record(record: DataRecordDict, errors: list[str]) -> None:
    """Write one rejected record to the rejected output CSV.

    Arguments:
        record: The original message that failed validation.
        errors: A list of validation error messages to include in the output.
    """
    append_csv_row(
        path=REJECTED_SALES_CSV,
        row=add_validation_errors(record=record, errors=errors),
        fieldnames=REJECTED_SALES_FIELDNAMES,
    )


def initialize_output() -> None:
    """Initialize output directory and clear rejected CSV from prior runs.

    This is not typical in production, but
    much better when experimenting.
    """
    LOG.info("Initializing output...")

    # If the output directory doesn't exist, create it.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # if the rejected CSV already exists from a prior run,
    # delete it and start fresh.
    if REJECTED_SALES_CSV.exists():
        REJECTED_SALES_CSV.unlink()

    LOG.info(f"Output directory ready: {OUTPUT_DIR.name}")


def send_messages(
    producer: Any,
    settings: KafkaSettings,
    valid_region_ids: set[str],
    valid_product_ids: set[str],
) -> tuple[int, int]:
    """Generate, validate, and send messages to the Kafka topic.

    For each message:
      - Validate it against the data contract.
      - If invalid, write it to the rejected CSV and skip.
      - If valid, send it to the Kafka topic and wait before the next one.

    Arguments:
        producer: An open Kafka producer.
        settings: Kafka settings including the topic name.
        valid_region_ids: Set of known region IDs for validation.
        valid_product_ids: Set of known product IDs for validation.

    Returns:
        A tuple of (sent_count, rejected_count).
    """
    LOG.info("Sending messages...")
    LOG.info(f"Sending up to {MESSAGE_COUNT} message(s) to topic {settings.topic!r}.")
    LOG.info("Watch each sale arrive. Press CTRL+C to stop early.\n")

    # initialize counters for summary stats at the end
    sent_count = 0
    rejected_count = 0

    # try to process messages, but catch any expected errors to log them and exit gracefully
    try:
        for message in generate_messages(MESSAGE_COUNT):
            LOG.info(format_message_for_log(message))

            result = validate_sale_record(
                record=message,
                valid_region_ids=valid_region_ids,
                valid_product_ids=valid_product_ids,
            )

            if not result.is_valid:
                rejected_count += 1
                LOG.warning("MESSAGE REJECTED")
                LOG.warning(f"  errors={result.errors}")
                write_rejected_record(message, result.errors)
                continue

            key = get_message_key(message)
            LOG.info(f"  Sending message with key={key}")

            produce_kafka_message(
                producer=producer,
                topic=settings.topic,
                key=key,
                message=message,
            )

            sent_count += 1
            LOG.info(f"  MESSAGE SENT  sent={sent_count}")
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    # except if we can't process messages
    # due to missing files, validation problems, or Kafka errors,
    # log the error and exit gracefully.
    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as error:
        LOG.error(str(error))
        LOG.error("Producer stopped before completing all messages.")
        raise SystemExit(1) from error

    # At the end, return the summary stats
    # with the count of messages sent and rejected.
    return sent_count, rejected_count


def log_rejected(rejected_count: int) -> None:
    """Log the rejected records CSV path if any records were rejected.

    Arguments:
        rejected_count: The number of rejected records.
    """
    LOG.info("Checking for rejected records...")
    if rejected_count > 0:
        log_path(LOG, "  WROTE REJECTED_SALES_CSV", REJECTED_SALES_CSV)
    else:
        LOG.info("  No records rejected.")


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(sent_count: int, rejected_count: int, settings: KafkaSettings) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Sent {sent_count} message(s) to topic {settings.topic!r}.")
    LOG.info(f"Rejected {rejected_count} message(s).")
    LOG.info("========================")
    LOG.info("Producer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka producer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    prepare_producer_topic(settings)
    valid_region_ids, valid_product_ids = load_reference_data()
    producer = create_producer(settings)

    LOG.info("========================")
    LOG.info("SECTION P. Produce Messages")
    LOG.info("========================")

    initialize_output()
    sent_count, rejected_count = send_messages(
        producer, settings, valid_region_ids, valid_product_ids
    )
    log_rejected(rejected_count)

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    producer.flush()
    log_summary(sent_count, rejected_count, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
