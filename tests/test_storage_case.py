"""Tests for streaming.storage.storage_case."""

from pathlib import Path

import duckdb

from streaming.storage.storage_case import (
    CONSUMED_REJECTED_FIELDNAMES,
    CONSUMED_VALID_FIELDNAMES,
    REJECTED_TABLE_NAME,
    VALID_TABLE_NAME,
    clean_rejected_consumed_record,
    clean_valid_consumed_record,
    clear_storage_tables,
    create_storage_tables,
    write_rejected_record,
    write_valid_record,
)

# === FIXTURES ===

SAMPLE_VALID_RECORD = {
    "order_id": "ORD-001",
    "datetime": "2026-05-08T10:00:00",
    "region_id": "US-MO",
    "currency_code": "USD",
    "product_id": "PROD-01",
    "unit_price": "29.99",
    "quantity": "2",
    "is_online": "true",
    "customer_id": "CUST-001",
    "is_new_customer": "false",
    "device_type": "mobile",
    "payment_method": "credit_card",
    "referral_source": "organic",
    "discount_code": "",
    "customer_note": "",
    "_kafka_key": "ORD-001",
    "_kafka_partition": "0",
    "_kafka_offset": "42",
}


# === clean_valid_consumed_record ===


def test_clean_valid_consumed_record_keeps_expected_fields() -> None:
    result = clean_valid_consumed_record(SAMPLE_VALID_RECORD)
    assert set(result.keys()) == set(CONSUMED_VALID_FIELDNAMES)


def test_clean_valid_consumed_record_fills_missing_with_empty() -> None:
    result = clean_valid_consumed_record({"order_id": "ORD-001"})
    assert result["region_id"] == ""


def test_clean_rejected_consumed_record_keeps_expected_fields() -> None:
    record = {**SAMPLE_VALID_RECORD, "validation_errors": "Missing field"}
    result = clean_rejected_consumed_record(record)
    assert set(result.keys()) == set(CONSUMED_REJECTED_FIELDNAMES)


# === create_storage_tables / clear_storage_tables ===


def test_create_storage_tables_creates_both_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    with duckdb.connect(str(db_path)) as conn:
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    assert VALID_TABLE_NAME in tables
    assert REJECTED_TABLE_NAME in tables


def test_clear_storage_tables_removes_all_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    clear_storage_tables(db_path)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 0


# === write_valid_record / write_rejected_record ===


def test_write_valid_record_inserts_row(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 1


def test_write_valid_record_stores_correct_values(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT order_id, region_id FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
    assert row is not None
    assert row[0] == "ORD-001"
    assert row[1] == "US-MO"


def test_write_rejected_record_inserts_with_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_rejected_record(db_path, SAMPLE_VALID_RECORD, ["Missing field: total"])
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 1


def test_write_multiple_records(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    record2 = {**SAMPLE_VALID_RECORD, "order_id": "ORD-002"}
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    write_valid_record(db_path, record2)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 2
