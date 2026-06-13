"""
Centralised SparkSession factory.

Keeps session config out of individual job files and makes unit-testing easy:
tests call get_spark(app_name, local=True) while EMR jobs call get_spark(app_name).
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession


def get_spark(
    app_name: str,
    local: bool = False,
    extra_conf: Optional[dict] = None,
) -> SparkSession:
    """Return a configured SparkSession.

    In local/test mode the session uses local[*] and in-memory catalog.
    In cluster mode (EMR) it picks up the cluster config automatically.

    Args:
        app_name:   Shown in the Spark UI and application history.
        local:      Force local mode (useful for unit tests).
        extra_conf: Additional Spark config key-value pairs to merge in.
    """
    builder = SparkSession.builder.appName(app_name)

    if local or os.environ.get("SPARK_ENV") == "local":
        builder = (
            builder.master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
        )
    else:
        # EMR / cluster defaults — cluster manager and executor memory are
        # configured externally via EMR step config, not here.
        builder = (
            builder
            .config("spark.sql.shuffle.partitions", "400")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            # Delta Lake / Iceberg support
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # Glue catalog for schema persistence across jobs
            .config(
                "spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            )
        )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
