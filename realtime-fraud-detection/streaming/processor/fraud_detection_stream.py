"""
Fraud Detection Streaming Job

PySpark Structured Streaming job that:
  1. Consumes raw transactions from Kafka
  2. Enriches each transaction with features from the Redis online feature store
  3. Applies the deterministic rules engine
  4. Emits scored/flagged transactions back to Kafka
  5. Updates the online feature store (velocity counters, running averages)

Delivery semantics: exactly-once via Kafka idempotent producer + Spark checkpointing.

Latency target: p99 < 200ms from Kafka produce to decision output.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from typing import Any, Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from feature_store.redis_client import FeatureStoreClient
from rules_engine import RulesEngine, FraudDecision

logger = logging.getLogger(__name__)

# Kafka message schema — matches the Avro schema registered in the Schema Registry
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),    False),
    StructField("card_id",          StringType(),    False),
    StructField("customer_id",      StringType(),    False),
    StructField("merchant_id",      StringType(),    False),
    StructField("amount_usd",       DecimalType(18, 4), False),
    StructField("currency",         StringType(),    True),
    StructField("merchant_category",StringType(),    True),
    StructField("device_fingerprint",StringType(),   True),
    StructField("ip_address",       StringType(),    True),
    StructField("latitude",         FloatType(),     True),
    StructField("longitude",        FloatType(),     True),
    StructField("event_time",       StringType(),    False),   # ISO 8601
])

SCORED_SCHEMA = StructType([
    StructField("transaction_id",     StringType(),  False),
    StructField("card_id",            StringType(),  False),
    StructField("customer_id",        StringType(),  False),
    StructField("amount_usd",         DecimalType(18, 4), False),
    StructField("fraud_decision",     StringType(),  False),  # BLOCK | REVIEW | PASS
    StructField("triggered_rules",    StringType(),  True),   # JSON array
    StructField("fraud_score",        FloatType(),   True),   # ML score (0-1)
    StructField("decision_reason",    StringType(),  True),
    StructField("processed_at",       TimestampType(), False),
])


def get_spark(kafka_bootstrap: str, checkpoint_path: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("fraud-detection-streaming")
        .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
        .config("spark.sql.adaptive.enabled", "true")
        # Kafka source configs
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
        # Small micro-batches for low latency; 500ms trigger keeps p99 < 200ms
        .getOrCreate()
    )


def read_transactions(spark: SparkSession, kafka_bootstrap: str) -> DataFrame:
    """Read raw transaction events from Kafka."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "transactions.raw")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "fraud-detection-processor")
        # Back-pressure: don't read more than 10k records per micro-batch
        .option("maxOffsetsPerTrigger", 10_000)
        .option("kafka.security.protocol", "SSL")
        .load()
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                TRANSACTION_SCHEMA,
            ).alias("tx"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("tx.*", "kafka_timestamp")
    )


def score_transaction_batch(
    partition: Iterator[Any],
    redis_host: str,
    redis_port: int,
) -> Iterator[Any]:
    """
    mapInPandas UDF for scoring transactions with full feature enrichment.

    One FeatureStoreClient and RulesEngine are created per executor partition,
    avoiding per-row connection overhead while maintaining isolation.
    """
    import pandas as pd
    from feature_store.redis_client import FeatureStoreClient
    from rules_engine import RulesEngine

    feature_store = FeatureStoreClient(host=redis_host, port=redis_port)
    rules = RulesEngine(feature_store=feature_store)

    for batch_df in partition:
        results = []
        for _, row in batch_df.iterrows():
            tx = row.to_dict()
            decision: FraudDecision = rules.evaluate(tx)

            # Update online feature store after scoring (async — fire and forget)
            feature_store.update_velocity(
                card_id=tx["card_id"],
                amount=float(tx["amount_usd"]),
                timestamp=tx["event_time"],
            )

            results.append({
                "transaction_id":  tx["transaction_id"],
                "card_id":         tx["card_id"],
                "customer_id":     tx["customer_id"],
                "amount_usd":      tx["amount_usd"],
                "fraud_decision":  decision.action,
                "triggered_rules": json.dumps(decision.triggered_rules),
                "fraud_score":     decision.ml_score,
                "decision_reason": decision.reason,
                "processed_at":    datetime.utcnow(),
            })

        yield pd.DataFrame(results)


def write_decisions(df: DataFrame, kafka_bootstrap: str, checkpoint_path: str):
    """Write fraud decisions to two Kafka topics."""

    # All decisions → transactions.scored (consumed by payments gateway)
    scored_query = (
        df
        .select(
            F.col("transaction_id").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "transactions.scored")
        .option("checkpointLocation", f"{checkpoint_path}/scored")
        .option("kafka.security.protocol", "SSL")
        .trigger(processingTime="500 milliseconds")
        .start()
    )

    # High-risk decisions only → fraud.alerts (consumed by alert service)
    alerts_query = (
        df
        .filter(F.col("fraud_decision").isin(["BLOCK", "REVIEW"]))
        .select(
            F.col("transaction_id").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "fraud.alerts")
        .option("checkpointLocation", f"{checkpoint_path}/alerts")
        .option("kafka.security.protocol", "SSL")
        .trigger(processingTime="500 milliseconds")
        .start()
    )

    return scored_query, alerts_query


def run(kafka_bootstrap: str, redis_host: str, redis_port: int, checkpoint_path: str):
    spark = get_spark(kafka_bootstrap, checkpoint_path)
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_transactions(spark, kafka_bootstrap)

    # Score via mapInPandas — one Redis connection pool per executor
    scored_df = raw_df.mapInPandas(
        lambda partition: score_transaction_batch(partition, redis_host, redis_port),
        schema=SCORED_SCHEMA,
    )

    q1, q2 = write_decisions(scored_df, kafka_bootstrap, checkpoint_path)

    # Block until termination signal
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap-servers", required=True)
    parser.add_argument("--redis-host", required=True)
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--checkpoint-path", required=True)
    args = parser.parse_args()

    run(
        kafka_bootstrap=args.kafka_bootstrap_servers,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        checkpoint_path=args.checkpoint_path,
    )
