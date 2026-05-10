"""src/streaming/data_validation/data_contract_case.py.

Defines what a valid message looks like for this project:
required fields, allowed values, reference table fields,
and output field order.

The reusable validation helpers live in core/validation_utils.py.
The domain-specific field rules and validate_sale_record live here.

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it data_contract_yourname.py, and modify your copy
  to adapt the rules for a different domain.
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
    """Validate one sale record against this project's data contract."""
    errors: list[str] = []

    # This function requires keyword arguments to make it clear
    # what reference data is needed for validation.
    errors.extend(
        validate_required_fields(record=record, required_fields=SALES_REQUIRED_FIELDS)
    )
    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    if record["region_id"] not in valid_region_ids:
        errors.append(f"Unknown region_id: {record['region_id']!r}")
    if record["product_id"] not in valid_product_ids:
        errors.append(f"Unknown product_id: {record['product_id']!r}")
    if record["device_type"] not in ALLOWED_DEVICE_TYPES:
        errors.append(f"Invalid device_type: {record['device_type']!r}")
    if record["payment_method"] not in ALLOWED_PAYMENT_METHODS:
        errors.append(f"Invalid payment_method: {record['payment_method']!r}")
    if record["referral_source"] not in ALLOWED_REFERRAL_SOURCES:
        errors.append(f"Invalid referral_source: {record['referral_source']!r}")
    if record["currency_code"] not in ALLOWED_CURRENCY_CODES:
        errors.append(f"Invalid currency_code: {record['currency_code']!r}")

    errors.extend(validate_datetime(record["datetime"]))
    errors.extend(validate_positive_integer(record["quantity"]))
    errors.extend(validate_boolean_text(record["is_online"], field_name="is_online"))
    errors.extend(
        validate_boolean_text(record["is_new_customer"], field_name="is_new_customer")
    )

    return ValidationResult(is_valid=not errors, errors=errors)


# === OUTPUT HELPERS ===


def keep_sales_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required sales fields in standard order."""
    return {field: row.get(field, "") for field in SALES_REQUIRED_FIELDS}
