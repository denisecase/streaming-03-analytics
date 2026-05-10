"""src/streaming/data_validation/data_validation_case.py.

Project-specific validation extensions.

Generic validation helpers live in datafun-streaming.
Add domain-specific validators here as requirements evolve.

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it data_validation_yourname.py, and modify your copy.
  Change __all__ to export your custom validators.
  Then, import your custom validators from your file.
"""

# === DECLARE IMPORTS ===

from datafun_streaming.data_validation.reference import (
    make_lookup_set,
    validate_reference_records,
)
from datafun_streaming.data_validation.validation_utils import add_validation_errors

# === DECLARE EXPORTS ===

# Use the built-in __all__ variable to declare a list of
# public objects that this module exports.
# This is a common Python convention that helps other developers understand
# which functions are intended for use outside this module.

__all__ = [
    "add_validation_errors",
    "make_lookup_set",
    "validate_quantity",
    "validate_reference_records",
]


# === DOMAIN-SPECIFIC VALIDATORS ===


def validate_quantity(value: str) -> list[str]:
    """Return errors for an invalid quantity value.

    All quantity values must be integers greater than or equal to 1.

    Arguments:
        value: The text value to validate.

    Returns:
        A list of errors, or an empty list if the value is valid.
    """
    try:
        quantity = int(value)
    except ValueError:
        return [f"Quantity must be an integer: {value}"]

    if quantity < 1:
        return [f"Quantity must be at least 1: {value}"]

    return []
