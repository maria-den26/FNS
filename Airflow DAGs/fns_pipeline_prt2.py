from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fns_pipeline_prt2',
    default_args=default_args,
    schedule_interval='0 20 2 * *',  # 20:00 второго числа каждого месяца
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['fns', 'postgres'],
) as dag:

    run_fns_sql_procedure = PostgresOperator(
        task_id='run_fns_pipeline_prt2',
        postgres_conn_id='pg_fns_conn',
        sql="""
        DO $$
        BEGIN
            CALL fns_storage.upload_fns_all_data();
            CALL fns_data.set_fns_hub();
            CALL fns_data.set_fns_link();
            CALL fns_data.set_fns_sat();
        END;
        $$;
        """
    )