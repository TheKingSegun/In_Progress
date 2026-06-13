"""
Gold layer: Customer Lifetime Value (LTV) computation.

This job runs daily and computes per-customer cumulative metrics over
the full history of Silver orders.  The output feeds the dim_customers
dbt model and the CRM/marketing platform via an export DAG.

Why Spark instead of dbt for this computation?
  - Historical scan over billions of order rows is faster with Spark's
    parallel columnar reads than Redshift's sequential scans.
  - The marketing team needs a flat export to S3 (Parquet) in addition
    to the Redshift table — one job, two outputs.

LTV model used: 12-month rolling window + all-time cumulative.
A more sophisticated BG/NBD probabilistic model lives in the ML platform;
this job produces the simpler rule-based LTV that operations teams use.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

import sys
sys.path.insert(0, "/opt/spark/jobs")

from utils.data_quality import DataQualityChecker
from utils.logger import get_logger
from utils.spark_session import get_spark

logger = get_logger(__name__)

COMPLETED_STATUSES = ["delivered", "shipped"]
EXCLUDED_STATUSES = ["cancelled", "refunded", "returned"]


def read_silver_orders(spark: SparkSession, silver_path: str) -> DataFrame:
    """Read full order history from Silver (all partitions)."""
    logger.info("Reading full silver order history from %s", silver_path)
    return spark.read.parquet(silver_path)


def compute_ltv_metrics(df: DataFrame, as_of_date: date) -> DataFrame:
    """
    Compute per-customer LTV metrics.

    Metrics:
      - total_orders              All-time order count (all statuses)
      - completed_orders          Orders successfully delivered/shipped
      - cancelled_orders          Orders cancelled/refunded/returned
      - total_revenue_usd         Gross revenue (completed orders only)
      - avg_order_value_usd       Average order value (completed orders)
      - first_order_date          Date of first ever order
      - last_order_date           Date of most recent order
      - days_since_last_order     Recency for churn scoring
      - revenue_l12m_usd          Revenue in last 12 months (rolling)
      - orders_l12m               Order count in last 12 months
      - ltv_segment               Rule-based LTV tier
    """
    cutoff_12m = date(as_of_date.year - 1, as_of_date.month, as_of_date.day)

    completed = df.filter(F.col("order_status").isin(COMPLETED_STATUSES))
    cancelled = df.filter(F.col("order_status").isin(EXCLUDED_STATUSES))
    last_12m = completed.filter(F.col("order_date") >= F.lit(str(cutoff_12m)))

    total_agg = (
        df
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    completed_agg = (
        completed
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("completed_orders"),
            F.sum("order_total_usd").cast(DecimalType(18, 2)).alias("total_revenue_usd"),
            F.avg("order_total_usd").cast(DecimalType(18, 2)).alias("avg_order_value_usd"),
        )
    )

    cancelled_agg = (
        cancelled
        .groupBy("customer_id")
        .agg(F.count("order_id").alias("cancelled_orders"))
    )

    l12m_agg = (
        last_12m
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("orders_l12m"),
            F.sum("order_total_usd").cast(DecimalType(18, 2)).alias("revenue_l12m_usd"),
        )
    )

    result = (
        total_agg
        .join(completed_agg, "customer_id", "left")
        .join(cancelled_agg, "customer_id", "left")
        .join(l12m_agg, "customer_id", "left")
        .fillna(0, subset=[
            "completed_orders", "cancelled_orders", "total_revenue_usd",
            "avg_order_value_usd", "orders_l12m", "revenue_l12m_usd",
        ])
        .withColumn(
            "days_since_last_order",
            F.datediff(F.lit(str(as_of_date)), F.col("last_order_date")),
        )
        .withColumn(
            "cancel_rate",
            F.round(
                F.col("cancelled_orders") / F.greatest(F.col("total_orders"), F.lit(1)),
                4,
            ),
        )
    )

    return assign_ltv_segment(result)


def assign_ltv_segment(df: DataFrame) -> DataFrame:
    """
    Rule-based LTV segmentation used by CRM for campaign targeting.

    Segments:
      - champions:      High revenue, recent, low cancel rate
      - loyal:          Consistent buyers over 12m
      - at_risk:        High historical LTV but hasn't purchased recently
      - new_customers:  First purchase in last 90 days
      - low_value:      Below average revenue and order count
    """
    return df.withColumn(
        "ltv_segment",
        F.when(
            (F.col("revenue_l12m_usd") >= 1000)
            & (F.col("days_since_last_order") <= 30)
            & (F.col("cancel_rate") < 0.1),
            "champions",
        )
        .when(
            (F.col("orders_l12m") >= 4) & (F.col("revenue_l12m_usd") >= 300),
            "loyal",
        )
        .when(
            (F.col("total_revenue_usd") >= 500)
            & (F.col("days_since_last_order") > 180),
            "at_risk",
        )
        .when(
            F.col("days_since_last_order") <= 90,
            "new_customers",
        )
        .otherwise("low_value"),
    )


def write_gold(df: DataFrame, gold_path: str, s3_export_path: str, as_of_date: date) -> None:
    df_persisted = df.persist()
    count = df_persisted.count()
    logger.info("Writing %d customer LTV records", count)

    # Write to S3 for Redshift COPY and direct ML platform consumption
    (
        df_persisted
        .repartition(10)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(f"{gold_path}/as_of_date={as_of_date}")
    )

    # Latest snapshot at a stable path for Redshift external table
    (
        df_persisted
        .repartition(10)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(f"{s3_export_path}/customer_ltv_latest")
    )

    df_persisted.unpersist()
    logger.info("Gold customer LTV write complete")


def run(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    s3_export_path: str,
    as_of_date: date,
) -> None:
    silver_df = read_silver_orders(spark, silver_path)
    ltv_df = compute_ltv_metrics(silver_df, as_of_date)

    DataQualityChecker(ltv_df, "gold_customer_ltv").check_not_empty().check_no_nulls(
        ["customer_id", "total_orders", "ltv_segment"]
    ).check_unique(["customer_id"]).check_values_in_set(
        "ltv_segment",
        ["champions", "loyal", "at_risk", "new_customers", "low_value"],
    ).validate(
        raise_on_failure=True
    )

    write_gold(ltv_df, gold_path, s3_export_path, as_of_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--gold-path", required=True)
    parser.add_argument("--export-path", required=True)
    args = parser.parse_args()

    as_of_date = date.fromisoformat(args.as_of_date)
    spark = get_spark("gold-customer-ltv")
    try:
        run(spark, args.silver_path, args.gold_path, args.export_path, as_of_date)
    finally:
        spark.stop()
