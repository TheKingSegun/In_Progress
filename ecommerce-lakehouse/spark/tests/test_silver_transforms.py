"""
Unit tests for silver/transform_orders.py.

Uses chispa for DataFrame equality assertions and pytest for test structure.
Tests cover the main transformation logic in isolation — no S3 reads/writes.
"""

from datetime import date

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import sys
sys.path.insert(0, "/opt/spark/jobs")

from jobs.silver.transform_orders import (
    VALID_CHANNELS,
    VALID_ORDER_STATUSES,
    add_derived_columns,
    add_usd_amount,
    cast_and_normalise,
    deduplicate,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_total_amount", StringType()),
        StructField("currency_code", StringType()),
        StructField("channel", StringType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
        StructField("shipping_country", StringType()),
        StructField("item_count", StringType()),
        StructField("_cdc_operation", StringType()),
        StructField("_ingestion_date", StringType()),
    ]
)


def make_bronze_row(
    order_id="ORD-001",
    customer_id="CUST-001",
    status="delivered",
    amount="150.00",
    currency="USD",
    channel="web",
    created_at="2024-03-15T10:30:00.000Z",
    updated_at="2024-03-15T14:00:00.000Z",
    country="US",
    items="3",
):
    return (order_id, customer_id, status, amount, currency, channel,
            created_at, updated_at, country, items, "c", "2024-03-15")


# ---------------------------------------------------------------------------
# cast_and_normalise
# ---------------------------------------------------------------------------

class TestCastAndNormalise:
    def test_types_are_cast_correctly(self, spark: SparkSession):
        df = spark.createDataFrame([make_bronze_row()], schema=BRONZE_SCHEMA)
        result = cast_and_normalise(df)

        field_types = {f.name: type(f.dataType) for f in result.schema.fields}
        assert field_types["order_total_amount"] == DecimalType
        assert field_types["item_count"] == IntegerType
        assert field_types["order_date"] == DateType

    def test_status_is_lowercased(self, spark: SparkSession):
        df = spark.createDataFrame(
            [make_bronze_row(status="  DELIVERED  ")], schema=BRONZE_SCHEMA
        )
        result = cast_and_normalise(df)
        assert result.first()["order_status"] == "delivered"

    def test_country_is_uppercased(self, spark: SparkSession):
        df = spark.createDataFrame(
            [make_bronze_row(country="  gb  ")], schema=BRONZE_SCHEMA
        )
        result = cast_and_normalise(df)
        assert result.first()["shipping_country"] == "GB"

    def test_order_date_extracted_from_created_at(self, spark: SparkSession):
        df = spark.createDataFrame(
            [make_bronze_row(created_at="2024-06-20T08:00:00.000Z")],
            schema=BRONZE_SCHEMA,
        )
        result = cast_and_normalise(df)
        assert result.first()["order_date"] == date(2024, 6, 20)


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_keeps_latest_updated_at(self, spark: SparkSession):
        rows = [
            make_bronze_row(
                order_id="ORD-001",
                status="confirmed",
                updated_at="2024-03-15T10:00:00.000Z",
            ),
            make_bronze_row(
                order_id="ORD-001",
                status="delivered",
                updated_at="2024-03-15T16:00:00.000Z",
            ),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        cast_df = cast_and_normalise(df)
        result = deduplicate(cast_df)

        assert result.count() == 1
        assert result.first()["order_status"] == "delivered"

    def test_no_duplicates_unchanged(self, spark: SparkSession):
        rows = [
            make_bronze_row(order_id="ORD-001"),
            make_bronze_row(order_id="ORD-002"),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        cast_df = cast_and_normalise(df)
        result = deduplicate(cast_df)
        assert result.count() == 2


# ---------------------------------------------------------------------------
# add_usd_amount
# ---------------------------------------------------------------------------

class TestAddUsdAmount:
    def test_usd_passthrough(self, spark: SparkSession):
        df = spark.createDataFrame(
            [make_bronze_row(amount="200.00", currency="USD")], schema=BRONZE_SCHEMA
        )
        result = add_usd_amount(cast_and_normalise(df))
        assert float(result.first()["order_total_usd"]) == pytest.approx(200.0, rel=1e-3)

    def test_eur_conversion(self, spark: SparkSession):
        df = spark.createDataFrame(
            [make_bronze_row(amount="100.00", currency="EUR")], schema=BRONZE_SCHEMA
        )
        result = add_usd_amount(cast_and_normalise(df))
        # EUR rate = 1.08
        assert float(result.first()["order_total_usd"]) == pytest.approx(108.0, rel=1e-2)


# ---------------------------------------------------------------------------
# add_derived_columns
# ---------------------------------------------------------------------------

class TestAddDerivedColumns:
    def test_high_value_flag(self, spark: SparkSession):
        rows = [
            make_bronze_row(order_id="ORD-001", amount="600.00", currency="USD"),
            make_bronze_row(order_id="ORD-002", amount="100.00", currency="USD"),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        result = add_derived_columns(add_usd_amount(cast_and_normalise(df)))

        by_id = {r["order_id"]: r for r in result.collect()}
        assert by_id["ORD-001"]["is_high_value"] is True
        assert by_id["ORD-002"]["is_high_value"] is False

    def test_order_size_tiers(self, spark: SparkSession):
        rows = [
            make_bronze_row(order_id="ORD-1", items="1"),
            make_bronze_row(order_id="ORD-2", items="3"),
            make_bronze_row(order_id="ORD-3", items="10"),
            make_bronze_row(order_id="ORD-4", items="25"),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        result = add_derived_columns(add_usd_amount(cast_and_normalise(df)))

        tiers = {r["order_id"]: r["order_size_tier"] for r in result.collect()}
        assert tiers["ORD-1"] == "single_item"
        assert tiers["ORD-2"] == "small_basket"
        assert tiers["ORD-3"] == "medium_basket"
        assert tiers["ORD-4"] == "large_basket"

    def test_international_flag(self, spark: SparkSession):
        rows = [
            make_bronze_row(order_id="ORD-US", country="US"),
            make_bronze_row(order_id="ORD-GB", country="GB"),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        result = add_derived_columns(add_usd_amount(cast_and_normalise(df)))

        intl = {r["order_id"]: r["is_international"] for r in result.collect()}
        assert intl["ORD-US"] is False
        assert intl["ORD-GB"] is True
