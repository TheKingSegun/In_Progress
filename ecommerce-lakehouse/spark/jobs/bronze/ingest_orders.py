"""
Bronze layer: Raw order ingestion from the operational database CDC stream.

Reads Debezium CDC events from S3 (landed by Kafka S3 Sink connector),
applies minimal schema enforcement, adds audit metadata, and writes
immutable Parquet partitioned by ingestion date.

Design principles:
- Bronze is IMMUTABLE. We never overwrite, only append.
- We parse just enough to partition correctly (event_date). No business logic.
- If a record doesn't conform to the envelope schema we quarantine it —
  we never silently drop data at this layer.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, "/opt/spark/jobs")

from utils.data_quality import DataQualityChecker
from utils.logger import get_logger
from utils.spark_session import get_spark

logger = get_logger(__name__)

# Debezium CDC envelope schema — the "after" payload carries the row state.
DEBEZIUM_ENVELOPE_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),         # c=create, u=update, d=delete
        StructField("ts_ms", StringType(), True),      # Kafka event timestamp
        StructField(
            "after",
            StructType(
                [
                    StructField("order_id", StringType(), False),
                    StructField("customer_id", StringType(), False),
                    StructField("order_status", StringType(), True),
                    StructField("order_total_amount", StringType(), True),
                    StructField("currency_code", StringType(), True),
                    StructField("channel", StringType(), True),
                    StructField("created_at", StringType(), True),
                    StructField("updated_at", StringType(), True),
                    StructField("shipping_country", StringType(), True),
                    StructField("item_count", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


def read_cdc_events(spark: SparkSession, source_path: str) -> DataFrame:
    """Read raw CDC JSON events from S3 landing zone."""
    logger.info("Reading CDC events from %s", source_path)
    return (
        spark.read
        .option("multiline", "false")
        .option("mode", "PERMISSIVE")       # malformed records go to _corrupt_record
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(DEBEZIUM_ENVELOPE_SCHEMA)
        .json(source_path)
    )


def quarantine_corrupt_records(df: DataFrame, quarantine_path: str) -> DataFrame:
    """Split out records that failed schema parsing and write them aside."""
    corrupt = df.filter(F.col("_corrupt_record").isNotNull())
    corrupt_count = corrupt.count()

    if corrupt_count > 0:
        logger.warning("Quarantining %d corrupt records to %s", corrupt_count, quarantine_path)
        (
            corrupt
            .withColumn("_quarantine_ts", F.current_timestamp())
            .coalesce(1)
            .write.mode("append")
            .json(quarantine_path)
        )

    return df.filter(F.col("_corrupt_record").isNull())


def flatten_and_enrich(df: DataFrame, run_date: date) -> DataFrame:
    """
    Flatten the Debezium envelope and add bronze audit columns.

    We keep the raw string types from source — Silver is responsible for casting.
    The only transformation here is unpacking the nested 'after' struct and
    adding metadata columns that are critical for auditing and replay.
    """
    return (
        df
        .filter(F.col("op").isin(["c", "u", "r"]))   # exclude tombstones (d)
        .select(
            F.col("after.order_id"),
            F.col("after.customer_id"),
            F.col("after.order_status"),
            F.col("after.order_total_amount"),
            F.col("after.currency_code"),
            F.col("after.channel"),
            F.col("after.created_at"),
            F.col("after.updated_at"),
            F.col("after.shipping_country"),
            F.col("after.item_count"),
            F.col("op").alias("_cdc_operation"),
            F.col("ts_ms").alias("_kafka_timestamp_ms"),
            # Audit metadata
            F.lit(str(run_date)).alias("_ingestion_date"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit("orders-cdc-s3-sink").alias("_source_system"),
        )
    )


def write_bronze(df: DataFrame, output_path: str, run_date: date) -> None:
    """Write to Bronze as Parquet, partitioned by ingestion date."""
    partition_path = f"{output_path}/ingestion_date={run_date}"
    logger.info("Writing bronze orders to %s", partition_path)

    # Repartition to avoid too many small files. At ~5M orders/day,
    # 200MB target file size → ~20 partitions for typical row sizes.
    (
        df
        .repartition(20)
        .write
        .mode("overwrite")          # idempotent — reruns replace the partition
        .option("compression", "snappy")
        .parquet(partition_path)
    )
    logger.info("Bronze write complete")


def run(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
    quarantine_path: str,
    run_date: date,
    expected_row_count: int = 0,
) -> None:
    raw = read_cdc_events(spark, source_path)
    clean = quarantine_corrupt_records(raw, quarantine_path)
    enriched = flatten_and_enrich(clean, run_date)

    # Quality gate before persisting
    checker = DataQualityChecker(enriched, "bronze_orders")
    checker.check_not_empty()
    checker.check_no_nulls(["order_id", "customer_id", "_ingestion_date"])

    if expected_row_count > 0:
        checker.check_volume_vs_previous(expected_row_count, tolerance_pct=0.35)

    checker.validate(raise_on_failure=True)

    write_bronze(enriched, bronze_path, run_date)

    logger.info(
        "Bronze ingestion complete",
        extra={"row_count": enriched.count(), "run_date": str(run_date)},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze order ingestion")
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--quarantine-path", required=True)
    parser.add_argument("--expected-rows", type=int, default=0)
    args = parser.parse_args()

    run_date = date.fromisoformat(args.run_date)
    spark = get_spark("bronze-orders-ingestion")

    try:
        run(
            spark=spark,
            source_path=args.source_path,
            bronze_path=args.bronze_path,
            quarantine_path=args.quarantine_path,
            run_date=run_date,
            expected_row_count=args.expected_rows,
        )
    finally:
        spark.stop()
