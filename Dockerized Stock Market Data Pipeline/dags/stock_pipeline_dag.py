import os
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL").split(",") if s.strip()]
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "5 * * * *")
AIRFLOW_TZ = os.getenv("AIRFLOW_TZ", "Asia/Kolkata")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECS = int(os.getenv("RETRY_DELAY_SECS", "10"))

# Make app importable
import sys
sys.path.append("/opt/airflow/app")
from fetch_and_upsert import fetch_and_store, RecoverableAPIError, IrrecoverableAPIError  # noqa: E402

with DAG(
    dag_id="stock_pipeline_dag",
    description="Fetch stock data from Alpha Vantage and upsert into Postgres",
    schedule=SCHEDULE_CRON,
    start_date=days_ago(1),
    catchup=False,
    max_active_tasks=1,  # be friendly to free API rate limits
    max_active_runs=1,
    default_args={
        "retries": MAX_RETRIES,
        "retry_delay": timedelta(seconds=RETRY_DELAY_SECS),
    },
    tags=["stocks", "alpha_vantage", "postgres"],
) as dag:

    @task
    def process_symbol(symbol: str) -> str:
        try:
            n = fetch_and_store(symbol)
            return f"{symbol}: upserted {n} rows"
        except RecoverableAPIError as e:
            # Mark as retry-able
            raise e
        except IrrecoverableAPIError as e:
            # Skip this run for this symbol but don't fail the DAG
            raise AirflowSkipException(str(e))

    # Dynamic task mapping across symbols
    process_symbol.expand(symbol=SYMBOLS)
