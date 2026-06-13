"""
Silver layer: Order data cleansing and enrichment.

Reads Bronze Parquet, applies:
  1. Type casting and normalisation
  2. Business rule validation (with quarantine for violators)
  3. Deduplication — CDC produces duplicate events; we keep the latest per order_id
  4. Currency normalisation to USD
  5. Derived columns used across multiple downstream models

Output is Delta Parquet on S3, partitioned by order_date (business date,
not ingestion date) — downstream dbt models query by order_date.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)

import sys
sys.path.insert(0, "/opt/spark/jobs")

from utils.data_quality import DataQualityChecker
from utils.logger import get_logger
from utils.spark_session import get_spark

logger = get_logger(__name__)

VALID_ORDER_STATUSES = [
    "pending", "confirmed", "processing", "shipped",
    "delivered", "cancelled", "refunded", "returned",
]

VALID_CHANNELS = ["web", "mobile_ios", "mobile_android", "marketplace", "in_store"]

# Simplified exchange rates — in production these come from a rates table
# populated by a separate FX rates pipeline.
FX_RATES_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "CAD": 0.74,
    "AUD": 0.65,
    "NGN": 0.00065,
}


def read_bronze(spark: SparkSession, bronze_path: str, run_date: date) -> DataFrame:
    partition_path = f"{bronze_path}/ingestion_date={run_date}"
    logger.info("Reading bronze from %s", partition_path)
    return spark.read.parquet(partition_path)


def cast_and_normalise(df: DataFrame) -> DataFrame:
    """Cast string columns from Bronze to their correct types."""
    return (
        df
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("customer_id", F.col("customer_id").cast(StringType()))
        .withColumn(
            "order_total_amount",
            F.col("order_total_amount").cast(DecimalType(18, 4)),
        )
        .withColumn("item_count", F.col("item_count").cast(IntegerType()))
        .withColumn(
            "order_created_at",
            F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn(
            "order_updated_at",
            F.to_timestamp(F.col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn("order_date", F.to_date(F.col("order_created_at")))
        .withColumn(
            "order_status",
            F.lower(F.trim(F.col("order_status"))),
        )
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn(
            "shipping_country",
            F.upper(F.trim(F.col("shipping_country"))),
        )
        .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
        .drop("created_at", "updated_at")
    )


def quarantine_invalid_records(
    df: DataFrame, quarantine_path: str, run_date: date
) -> DataFrame:
    """
    Identify records that violate business rules and write them to quarantine.
    Returns only valid records.
    """
    invalid_mask = (
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("order_total_amount").isNull()
        | (F.col("order_total_amount") < 0)
        | ~F.col("order_status").isin(VALID_ORDER_STATUSES)
        | ~F.col("channel").isin(VALID_CHANNELS)
        | F.col("order_date").isNull()
    )

    invalid = df.filter(invalid_mask)
    invalid_count = invalid.count()

    if invalid_count > 0:
        logger.warning("Quarantining %d invalid silver records", invalid_count)
        (
            invalid
            .withColumn("_quarantine_reason", F.lit("business_rule_violation"))
            .withColumn("_quarantine_date", F.lit(str(run_date)))
            .write.mode("append")
            .parquet(f"{quarantine_path}/ingestion_date={run_date}")
        )

    return df.filter(~invalid_mask)


def deduplicate(df: DataFrame) -> DataFrame:
    """
    CDC can produce multiple events per order_id within a day (multiple updates).
    Keep the record with the latest order_updated_at timestamp per order_id.

    We use a Window function over order_id, ordered by updated_at DESC,
    and take row_number == 1 — this is more explicit than last() and handles
    ties deterministically.
    """
    w = Window.partitionBy("order_id").orderBy(F.col("order_updated_at").desc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def add_usd_amount(df: DataFrame) -> DataFrame:
    """Normalise order amounts to USD for cross-currency reporting."""
    fx_expr = F.create_map(
        *[item for pair in FX_RATES_TO_USD.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    return (
        df
        .withColumn(
            "order_total_usd",
            F.round(
                F.col("order_total_amount") * fx_expr[F.col("currency_code")],
                4,
            ),
        )
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "is_high_value",
            F.when(F.col("order_total_usd") >= 500, True).otherwise(False),
        )
        .withColumn(
            "order_size_tier",
            F.when(F.col("item_count") == 1, "single_item")
            .when(F.col("item_count") <= 5, "small_basket")
            .when(F.col("item_count") <= 20, "medium_basket")
            .otherwise("large_basket"),
        )
        .withColumn(
            "is_international",
            ~F.col("shipping_country").isin(["US", "USA"]),
        )
    )


def write_silver(df: DataFrame, silver_path: str, run_date: date) -> None:
    """
    Write Silver as Parquet partitioned by order_date.
    We overwrite the single date partition — idempotent reruns are safe.
    """
    logger.info("Writing silver orders to %s", silver_path)
    (
        df
        .repartition(20, F.col("order_date"))
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("order_date")
        .parquet(silver_path)
    )
    logger.info("Silver write complete")


def run(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
    run_date: date,
) -> None:
    bronze_df = read_bronze(spark, bronze_path, run_date)
    cast_df = cast_and_normalise(bronze_df)
    valid_df = quarantine_invalid_records(cast_df, quarantine_path, run_date)
    deduped_df = deduplicate(valid_df)
    enriched_df = add_usd_amount(deduped_df)
    final_df = add_derived_columns(enriched_df)

    DataQualityChecker(final_df, "silver_orders").check_not_empty().check_no_nulls(
        ["order_id", "customer_id", "order_date", "order_total_usd"]
    ).check_unique(["order_id"]).check_values_in_set(
        "order_status", VALID_ORDER_STATUSES
    ).validate(
        raise_on_failure=True
    )

    write_silver(final_df, silver_path, run_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--quarantine-path", required=True)
    args = parser.parse_args()

    run_date = date.fromisoformat(args.run_date)
    spark = get_spark("silver-orders-transform")
    try:
        run(spark, args.bronze_path, args.silver_path, args.quarantine_path, run_date)
    finally:
        spark.stop()
