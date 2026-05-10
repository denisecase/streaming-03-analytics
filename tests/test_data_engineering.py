# tests/test_data_engineering.py
"""Tests for data engineering calculations."""

from streaming.data_engineering.derived_fields import (
    TAX_RATE_DEFAULT,
    compute_tax_amount,
    compute_total_price,
    enrich_message,
    get_tax_rate,
)


def test_compute_total_price_multiplies_quantity_by_unit_price() -> None:
    """Total price should be quantity times unit price."""
    quantity = 3
    unit_price = 4.25

    result = compute_total_price(quantity, unit_price)

    assert result == round(quantity * unit_price, 2)


def test_compute_tax_amount_multiplies_total_price_by_tax_rate() -> None:
    """Tax amount should be total price times tax rate."""
    total_price = 25.00
    tax_rate = 0.08

    result = compute_tax_amount(total_price, tax_rate)

    assert result == round(total_price * tax_rate, 2)


def test_get_tax_rate_converts_percent_to_decimal() -> None:
    """Tax lookup values should be converted from percent to decimal."""
    region_lookup = {"TEST-REGION": 7.5}

    result = get_tax_rate("TEST-REGION", region_lookup)

    assert result == 0.075


def test_get_tax_rate_uses_default_for_unknown_region() -> None:
    """Unknown regions should use the default tax rate."""
    region_lookup = {"OTHER-REGION": 6.0}

    result = get_tax_rate("MISSING-REGION", region_lookup)

    assert result == TAX_RATE_DEFAULT


def test_enrich_message_adds_subtotal_tax_amount_and_total() -> None:
    """Enriched messages should include calculated sales fields."""
    row = {
        "region_id": "TEST-REGION",
        "quantity": 2,
        "unit_price": 10.00,
    }
    region_lookup = {"TEST-REGION": 8.0}

    enriched = enrich_message(row, region_lookup)

    expected_subtotal = round(row["quantity"] * row["unit_price"], 2)
    expected_tax_amount = round(expected_subtotal * 0.08, 2)
    expected_total = round(expected_subtotal + expected_tax_amount, 2)

    assert enriched["subtotal"] == expected_subtotal
    assert enriched["tax_amount"] == expected_tax_amount
    assert enriched["total"] == expected_total


def test_enrich_message_keeps_original_fields() -> None:
    """Enriched messages should preserve original message fields."""
    row = {
        "message_id": "example-message",
        "region_id": "TEST-REGION",
        "quantity": 1,
        "unit_price": 5.00,
    }
    region_lookup = {"TEST-REGION": 8.0}

    enriched = enrich_message(row, region_lookup)

    assert enriched["message_id"] == row["message_id"]
    assert enriched["region_id"] == row["region_id"]
    assert enriched["quantity"] == row["quantity"]
    assert enriched["unit_price"] == row["unit_price"]
