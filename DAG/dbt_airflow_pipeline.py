from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'tpch_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    staging_test = BashOperator(
        task_id='test_staging_source_tables',
        bash_command='cd /opt/airflow/dbt && dbt test --models staging.* --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    stg_orders = BashOperator(
        task_id='stg_orders_view',
        bash_command='cd /opt/airflow/dbt && dbt run --models staging_tpch_orders --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    stg_lineitem = BashOperator(
        task_id='stg_lineitem_view',
        bash_command='cd /opt/airflow/dbt && dbt run --models stg_lineitem --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    order_items = BashOperator(
        task_id='order_items_mart',
        bash_command='cd /opt/airflow/dbt && dbt run --models order_items_mart --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    order_summary = BashOperator(
        task_id='order_summary',
        bash_command='cd /opt/airflow/dbt && dbt run --models order_summary_mart --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    fact_orders = BashOperator(
        task_id='fact_orders',
        bash_command='cd /opt/airflow/dbt && dbt run --models fact_orders --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    mart_test = BashOperator(
        task_id='generic_test',
        bash_command='cd /opt/airflow/dbt && dbt test --models mart.* --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    singular_test = BashOperator(
        task_id='singular_test',
        bash_command='cd /opt/airflow/dbt && dbt test --select singular_test --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    valid_date = BashOperator(
        task_id='is_date_valid',
        bash_command='cd /opt/airflow/dbt && dbt test --select test_valid_date --profiles-dir /opt/airflow/dbt --no-partial-parse'
    )

    staging_test >> [stg_orders, stg_lineitem]
    [stg_orders, stg_lineitem] >> order_items
    order_items >> order_summary
    [stg_orders, order_summary] >> fact_orders
    fact_orders >> [mart_test, singular_test, valid_date]
