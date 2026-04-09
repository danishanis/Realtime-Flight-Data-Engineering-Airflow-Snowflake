
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Pointing to project root inside Airflow Container
AIRFLOW_HOME = Path("/opt/airflow")

# Preventing duplicate path entries
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))
    logger.info(
        "Registered AIRFLOW_HOME in sys.path: %s", AIRFLOW_HOME
    )

from scripts.bronze_layer import run_bronze_ingestion
# from scripts.silver_layer import run_silver_transform
# from scripts.gold_layer import run_gold_layer
# from scripts.snowflake_implement import snowflake_load

default_args = {
    "owner": "danish",

    "email_on_failure": False,
    "email_on_retry": False,

    "retries": 0, # if ingestion fails, we don't want to retry unnecessarily
    "retry_delay" : timedelta(minutes=5), # waiting 5 mins before retry
}

# DAG Workflow Definition
with DAG(
    dag_id="flights_ops_medallion_pipe",
    default_args=default_args,

    description="Ingests real-time flight data from OpenSky Network API \
                 through Bronze → Silver → Gold → Snowflake",

    start_date=datetime(2025, 12, 10),
    schedule_interval="*/30 * * * *",
    
    # To not try to backfill all historical runs since the start date
    # Without this, all historical runs would trigger immediately on first deployment
    catchup=False,

    tags=["flight-data", "medallion", "opensky"],
) as dag:

    # Bronze Task 
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
        provide_context=True,

        doc_md="""
        ### Bronze Ingestion Task
        Polls the OpenSky Network `/states/all` endpoint and persists the 
        raw API response as a timestamped JSON file in the Bronze landing zone.
        Pushes the file path to XCom under key `bronze_file`.
        """,
    )

    # Silver Task
    # silver = PythonOperator(
    #     task_id = "silver_transform",
    #     python_callable=run_silver_transform,
    #     provide_context=True,

    #     doc_md="""
    #     ### Silver Transform Task
    #     Reads Bronze JSON via XCom path, applies validation,
    #     deduplication, and type enforcement. Outputs clean Parquet
    #     to /data/silver/ and pushes path to XCom.
    #     """,
    # )

    # Gold Task
    # gold = PythonOperator(
    #     task_id = "gold_layer",
    #     python_callable=run_gold_layer,
    #     provide_context=True,

    #     doc_md="""
    #     ### Gold Aggregation Task
    #     Reads Silver Parquet via XCom path, applies business
    #     aggregations (routes, activity summaries). Outputs to
    #     /data/gold/ and pushes path to XCom.
    #     """,
    # )

    # Snowflake Load Task
    # snowflake = PythonOperator(
    #     task_id = "snowflake_load",
    #     python_callable=snowflake_load,
    #     provide_context=True,

    #     doc_md="""
    #     ### Snowflake Load Task
    #     Reads Gold data via XCom path and performs a MERGE
    #     operation into the Snowflake analytics schema.
    #     """,
    # )  


    # bronze >> silver >> gold >> snowflake
