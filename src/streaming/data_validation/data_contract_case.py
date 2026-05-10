"""src/streaming/data_validation/data_contract_case.py.

Defines what a valid message looks like for this project:
required fields, allowed values, reference table fields,
and output field order.

Use the data/*.csv files as the source of truth for the data contract.

The reusable validation helpers live in core/validation_utils.py.
The domain-specific field rules and validate_sale_record live here.

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it data_contract_yourname.py, and modify your copy
  to adapt the rules for a different domain.
  Then, import from your data_contract instead.
"""

# === DECLARE IMPORTS ===

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_boolean_text,
    validate_datetime,
    validate_positive_integer,
    validate_required_fields,
)

# === DECLARE REQUIRED FIELDS ===

# This is how Python knows exactly what fields are required
# for a valid message in this project.

# Ensuring messages meet the data contract is a critical part
# of building a reliable streaming pipelines.

# If messages don't meet the contract,
# downstream processes may fail or produce incorrect results.

# === EVENT TABLE FIELDS ===

SALES_REQUIRED_FIELDS: Final[list[str]] = [
    "order_id",
    "datetime",
    "region_id",
    "currency_code",
    "product_id",
    "unit_price",
    "quantity",
    "is_online",
    "customer_id",
    "payment_method",
]

SALES_OPTIONAL_FIELDS: Final[list[str]] = [
    "is_new_customer",
    "device_type",
    "referral_source",
    "discount_code",
    "customer_note",
]

# Build the full list of valid fieldnames for sales messages
# by using the asterisk or "splat" operator or "unpacking" operator
# to expand the required and optional fields
# so they can be combined in one list.
VALID_SALES_FIELDNAMES: Final[list[str]] = [
    *SALES_REQUIRED_FIELDS,
    *SALES_OPTIONAL_FIELDS,
]


# === REFERENCE TABLE FIELDS ===

REGIONS_REQUIRED_FIELDS: Final[list[str]] = [
    "region_id",
    "region_name",
    "country_code",
    "country_name",
    "currency_code",
    "tax_rate_pct",
    "timezone",
]

PRODUCTS_REQUIRED_FIELDS: Final[list[str]] = [
    "product_id",
    "product_name",
    "category",
    "level",
    "price_usd",
    "instructor",
]

CURRENCIES_REQUIRED_FIELDS: Final[list[str]] = [
    "currency_code",
    "currency_name",
    "symbol",
    "exchange_rate_to_usd",
    "rate_date",
]

DISCOUNT_CODES_REQUIRED_FIELDS: Final[list[str]] = [
    "discount_code",
    "discount_pct",
    "valid_from",
    "valid_to",
    "description",
]

# === ALLOWED VALUES ===

ALLOWED_DEVICE_TYPES: Final[set[str]] = {"mobile", "desktop", "tablet"}
ALLOWED_PAYMENT_METHODS: Final[set[str]] = {
    "credit_card",
    "paypal",
    "apple_pay",
    "gift_card",
}
ALLOWED_REFERRAL_SOURCES: Final[set[str]] = {
    "organic",
    "paid_search",
    "email",
    "social",
}
ALLOWED_CURRENCY_CODES: Final[set[str]] = {"USD", "CAD", "MXN"}

# === OUTPUT FIELD ORDER ===

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *SALES_REQUIRED_FIELDS,
    "subtotal",
    "tax_amount",
    "total",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_SALES_FIELDNAMES: Final[list[str]] = [
    *SALES_REQUIRED_FIELDS,
    "validation_errors",
]


# === DOMAIN-SPECIFIC VALIDATION ===


def validate_sale_record(
    *,
    record: DataRecordDict,
    valid_region_ids: set[str],
    valid_product_ids: set[str],
) -> ValidationResult:
    """Validate one sale record against this project's data contract.

    This function can be enhanced.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        record: The message to validate.
        valid_region_ids: The set of valid region_id values from the regions reference table.
        valid_product_ids: The set of valid product_id values from the products reference table.

    Returns:
        A ValidationResult indicating whether the record is valid and any errors found.
    """
    # Initialize an empty list to collect validation errors.
    errors: list[str] = []

    # Validate the required fields, get a list back,
    # and extend the errors list with any errors found.
    # This is a concise form of:
    # required_field_errors = validate_required_fields(record=record, required_fields=SALES_REQUIRED_FIELDS)
    # errors.extend(required_field_errors)
    # Use whichever you prefer.
    errors.extend(
        validate_required_fields(record=record, required_fields=SALES_REQUIRED_FIELDS)
    )

    if errors:
        # if there are errors,
        # return a ValidationResult with is_valid=False and the list of errors
        # That will exit this function early
        # and skip the rest of the validation checks below.
        return ValidationResult(is_valid=False, errors=errors)

    # If we get here, we know there were no errors
    # based on the validation checks we implemented so far.

    # Now, check the values of specific fields against allowed values or reference tables.
    # If any of these checks fail, add an error message to the errors list.

    # Do an experiment to figure out what the !r does in the f-strings below,
    # and then add a comment to explain it.

    # If the id value in the record is not in the set of valid ids,
    # add an error message to the errors list that includes the invalid value.
    if record["region_id"] not in valid_region_ids:
        errors.append(f"Unknown region_id: {record['region_id']!r}")

    if record["product_id"] not in valid_product_ids:
        errors.append(f"Unknown product_id: {record['product_id']!r}")

    # Check more fields against allowed values and add errors as needed.

    if record["device_type"] not in ALLOWED_DEVICE_TYPES:
        errors.append(f"Invalid device_type: {record['device_type']!r}")

    if record["payment_method"] not in ALLOWED_PAYMENT_METHODS:
        errors.append(f"Invalid payment_method: {record['payment_method']!r}")

    if record["referral_source"] not in ALLOWED_REFERRAL_SOURCES:
        errors.append(f"Invalid referral_source: {record['referral_source']!r}")

    if record["currency_code"] not in ALLOWED_CURRENCY_CODES:
        errors.append(f"Invalid currency_code: {record['currency_code']!r}")

    # Use append() to add one error message.
    # Use extend() to add several error messages from a list.
    # Our validation functions return a list of error messages.

    errors.extend(validate_datetime(record["datetime"]))

    errors.extend(validate_positive_integer(record["quantity"]))

    errors.extend(validate_boolean_text(record["is_online"], field_name="is_online"))

    errors.extend(
        validate_boolean_text(record["is_new_customer"], field_name="is_new_customer")
    )

    # After all checks, if the errors list is empty, the record is valid.
    # If there are any errors, the record is invalid.

    # Use Python truthiness on a list to see if we're valid.
    # If a list is empty, it is falsy; like it doesn't fully exist.
    # If a list has items, it is truthy; it more practically exists.
    has_errors = bool(errors)  # if there are any errors, this will be True

    # Use the Python not operator.
    is_result_valid = not has_errors  # if there are no errors, the record is valid

    return ValidationResult(is_valid=is_result_valid, errors=errors)


# === OUTPUT HELPERS ===


def keep_sales_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required sales fields in standard order.

    This is used to create the output message for both valid and rejected records.

    Arguments:
        row: The original message as a dict.

    Returns:
        A new dict with only the required fields in the standard order.
    """
    return {field: row.get(field, "") for field in SALES_REQUIRED_FIELDS}
