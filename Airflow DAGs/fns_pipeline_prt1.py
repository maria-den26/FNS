from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Указываем путь к проекту
project_path = '/mnt/d/GitHub/FNS/Parser'
sys.path.append(project_path)

# Импортируем main после добавления пути
def run_main():
    import main
    main.main()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='fns_pipeline_prt1',
    default_args=default_args,
    description='Ежемесячный запуск Parser/main.py',
    schedule_interval='0 20 1 * *',  # Каждое 1-е число месяца в 20:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['fns', 'parser', 'monthly'],
) as dag:
    
    run_fns_pipeline_prt1 = PythonOperator(
        task_id='run_fns_pipeline_prt1',
        python_callable=run_main,
    )

    run_fns_pipeline_prt1
