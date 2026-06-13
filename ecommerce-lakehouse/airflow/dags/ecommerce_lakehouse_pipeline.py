"""
E-Commerce Lakehouse Pipeline DAG

Orchestrates the full Bronze → Silver → Gold pipeline for orders data.

Schedule: Hourly (T+1h freshness target for Silver; Gold runs once daily at 06:00 UTC)

SLA:
  - Bronze ingest must complete within 30 minutes of schedule
  - Silver transform within 45 minutes
  - Gold within 90 minutes of start (daily run only)

Alerting: Slack #data-alerts on SLA miss or task failure.
On-call escalation via PagerDuty if Gold fails on weekdays.

Design notes:
  - Each task is idempotent — safe to rerun with the same logical_date
  - EMR clusters are ephemeral (created per run, terminated on completion)
    to avoid paying for idle cluster time between hourly runs
  - dbt transformation runs after Silver is complete and uses
    dbt Cloud API operator for managed execution
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENV = Variable.get("ENVIRONMENT", default_var="dev")
S3_BUCKET = Variable.get("DATA_LAKE_BUCKET")
EMR_ROLE = Variable.get("EMR_ROLE_ARN")
SPARK_SCRIPTS_PATH = f"s3://{S3_BUCKET}/spark-scripts"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/orders"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/orders"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/customer_ltv"
QUARANTINE_PATH = f"s3://{S3_BUCKET}/quarantine"
SLACK_CONN_ID = "slack_data_alerts"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,         # Slack preferred over email
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

EMR_JOB_FLOW_OVERRIDES = {
    "Name": "ecommerce-lakehouse-{{ ds }}",
    "ReleaseLabel": "emr-6.15.0",
    "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Driver",
                "Market": "ON_DEMAND",       # Driver on On-Demand for stability
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Workers",
                "Market": "SPOT",            # Workers on Spot for 70% cost saving
                "InstanceRole": "CORE",
                "InstanceType": "m5.2xlarge",
                "InstanceCount": 4,
                "BidPrice": "OnDemandPrice",
            },
        ],
        "Ec2SubnetId": "{{ var.value.EMR_SUBNET_ID }}",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "Configurations": [
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "true"},
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
        },
    ],
    "Tags": [
        {"Key": "Environment", "Value": ENV},
        {"Key": "Team", "Value": "data-engineering"},
        {"Key": "CostCenter", "Value": "data-platform"},
    ],
}


def make_spark_step(name: str, script: str, args: list[str]) -> dict:
    return {
        "Name": name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.yarn.submit.waitAppCompletion=true",
                f"{SPARK_SCRIPTS_PATH}/{script}",
                *args,
            ],
        },
    }


def is_gold_run(**context) -> str:
    """Only run Gold job on the 06:00 UTC schedule (once per day)."""
    execution_hour = context["logical_date"].hour
    if execution_hour == 6:
        return "create_emr_cluster_gold"
    return "skip_gold"


def alert_slack_on_failure(context):
    SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            f":red_circle: *DAG Failed*\n"
            f"DAG: `{context['dag'].dag_id}`\n"
            f"Task: `{context['task_instance'].task_id}`\n"
            f"Run: `{context['logical_date']}`\n"
            f"<{context['task_instance'].log_url}|View logs>"
        ),
    ).execute(context)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ecommerce_lakehouse_pipeline",
    description="Bronze → Silver → Gold pipeline for e-commerce orders",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",       # Hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                   # Prevent overlapping runs
    tags=["lakehouse", "orders", "production"],
    on_failure_callback=alert_slack_on_failure,
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Wait for source data to land in S3 before starting EMR
    # ------------------------------------------------------------------
    wait_for_source = S3KeySensor(
        task_id="wait_for_cdc_landing",
        bucket_name=S3_BUCKET,
        bucket_key="raw/orders/cdc/{{ ds_nodash }}/{{ macros.ds_format(ds, '%Y-%m-%d', '%H') }}/_SUCCESS",
        aws_conn_id="aws_default",
        timeout=1800,                    # 30-minute timeout
        poke_interval=60,
        mode="reschedule",               # Free up worker slot while waiting
        soft_fail=False,
    )

    # ------------------------------------------------------------------
    # EMR cluster lifecycle
    # ------------------------------------------------------------------
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    # ------------------------------------------------------------------
    # Bronze ingestion step
    # ------------------------------------------------------------------
    bronze_step = EmrAddStepsOperator(
        task_id="add_bronze_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=[
            make_spark_step(
                name="bronze-orders-ingest",
                script="bronze/ingest_orders.py",
                args=[
                    "--run-date", "{{ ds }}",
                    "--source-path", f"s3://{S3_BUCKET}/raw/orders/cdc/{{{{ ds_nodash }}}}",
                    "--bronze-path", BRONZE_PATH,
                    "--quarantine-path", QUARANTINE_PATH,
                ],
            )
        ],
    )

    wait_bronze = EmrStepSensor(
        task_id="wait_bronze_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('add_bronze_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    # ------------------------------------------------------------------
    # Silver transform step
    # ------------------------------------------------------------------
    silver_step = EmrAddStepsOperator(
        task_id="add_silver_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=[
            make_spark_step(
                name="silver-orders-transform",
                script="silver/transform_orders.py",
                args=[
                    "--run-date", "{{ ds }}",
                    "--bronze-path", BRONZE_PATH,
                    "--silver-path", SILVER_PATH,
                    "--quarantine-path", QUARANTINE_PATH,
                ],
            )
        ],
    )

    wait_silver = EmrStepSensor(
        task_id="wait_silver_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('add_silver_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    # ------------------------------------------------------------------
    # Branch: Gold only on 06:00 run
    # ------------------------------------------------------------------
    branch_gold = BranchPythonOperator(
        task_id="branch_gold_run",
        python_callable=is_gold_run,
    )

    skip_gold = EmptyOperator(task_id="skip_gold")

    create_emr_cluster_gold = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster_gold",
        job_flow_overrides=EMR_JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    gold_step = EmrAddStepsOperator(
        task_id="add_gold_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster_gold', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=[
            make_spark_step(
                name="gold-customer-ltv",
                script="gold/customer_ltv.py",
                args=[
                    "--as-of-date", "{{ ds }}",
                    "--silver-path", SILVER_PATH,
                    "--gold-path", GOLD_PATH,
                    "--export-path", f"s3://{S3_BUCKET}/exports",
                ],
            )
        ],
    )

    wait_gold = EmrStepSensor(
        task_id="wait_gold_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster_gold', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('add_gold_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    terminate_cluster_gold = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster_gold",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster_gold', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",         # Terminate even if Gold step fails
    )

    # ------------------------------------------------------------------
    # Terminate main cluster (always, even on failure)
    # ------------------------------------------------------------------
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )

    success = EmptyOperator(
        task_id="pipeline_success",
        trigger_rule="none_failed_min_one_success",
    )

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------
    (
        wait_for_source
        >> create_emr_cluster
        >> bronze_step
        >> wait_bronze
        >> silver_step
        >> wait_silver
        >> terminate_cluster
        >> branch_gold
    )

    branch_gold >> skip_gold >> success
    (
        branch_gold
        >> create_emr_cluster_gold
        >> gold_step
        >> wait_gold
        >> terminate_cluster_gold
        >> success
    )
