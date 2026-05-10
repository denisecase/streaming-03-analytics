"""Tests for streaming.storage.duckdb_sql."""

from datafun_streaming.storage.duckdb_sql import (
    build_clear_table_sql,
    build_create_table_sql,
    build_insert_sql,
)

# === build_create_table_sql ===


def test_build_create_table_sql_contains_table_name() -> None:
    sql = build_create_table_sql("sales", ["order_id", "region_id"])
    assert "sales" in sql


def test_build_create_table_sql_contains_if_not_exists() -> None:
    sql = build_create_table_sql("sales", ["order_id"])
    assert "IF NOT EXISTS" in sql


def test_build_create_table_sql_contains_all_fields() -> None:
    sql = build_create_table_sql("sales", ["order_id", "region_id", "total"])
    assert "order_id VARCHAR" in sql
    assert "region_id VARCHAR" in sql
    assert "total VARCHAR" in sql


def test_build_create_table_sql_single_field() -> None:
    sql = build_create_table_sql("test_table", ["id"])
    assert "id VARCHAR" in sql


# === build_clear_table_sql ===


def test_build_clear_table_sql_contains_table_name() -> None:
    sql = build_clear_table_sql("sales")
    assert "sales" in sql


def test_build_clear_table_sql_is_delete() -> None:
    sql = build_clear_table_sql("sales")
    assert sql.startswith("DELETE FROM")


# === build_insert_sql ===


def test_build_insert_sql_contains_table_name() -> None:
    sql = build_insert_sql("sales", ["order_id", "region_id"])
    assert "sales" in sql


def test_build_insert_sql_contains_all_fields() -> None:
    sql = build_insert_sql("sales", ["order_id", "region_id"])
    assert "order_id" in sql
    assert "region_id" in sql


def test_build_insert_sql_placeholder_count_matches_fields() -> None:
    fields = ["order_id", "region_id", "total"]
    sql = build_insert_sql("sales", fields)
    assert sql.count("?") == len(fields)


def test_build_insert_sql_is_insert() -> None:
    sql = build_insert_sql("sales", ["order_id"])
    assert sql.startswith("INSERT INTO")
