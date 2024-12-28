from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract import fetch_crash_data,generate_moon_phases
from etl.transform import transform_data
from etl.load import load_to_postgres,visualise
from airflow.models import Variable

# Константы
WORK_DIR = "/tmp"
MOON_DATA_FILE='/moon_phases.csv'
CRASHES_DATA_FILE='/crashes_data.csv'
TRANSFORMED_DATA_FILE = "/transformed_data.csv"
GRAPH_HTML = "/output_graph.html"
START_DATE = Variable.get("start_date", default_var="2024-01-01")
END_DATE = Variable.get("end_date", default_var="2024-12-01")

# Определение аргументов по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'nyc_crash_analysis',
    default_args=default_args,
    description='ETL pipeline for NYC crash data with moon phases',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_crashes_task = PythonOperator(
        task_id='extract_crash_data',
        python_callable=fetch_crash_data,
        op_kwargs={
            'start_date': datetime.strptime(START_DATE, '%Y-%m-%d'),
            'end_date': datetime.strptime(END_DATE, '%Y-%m-%d'),
            'output_file': WORK_DIR+CRASHES_DATA_FILE,
        },
    )

    extract_moon_phases_task = PythonOperator(
        task_id='generate_moon_phases',
        python_callable=generate_moon_phases,
        op_kwargs={
            'start_date': datetime.strptime(START_DATE, '%Y-%m-%d'),
            'end_date': datetime.strptime(END_DATE, '%Y-%m-%d'),
            'output_file': WORK_DIR+MOON_DATA_FILE,
        },
    )

    transform_task = PythonOperator(
        task_id='transform_crash_data',
        python_callable=transform_data,
        op_kwargs={
            'crash_file': WORK_DIR+CRASHES_DATA_FILE,
            'moon_file': WORK_DIR+MOON_DATA_FILE,
            'output_file':WORK_DIR+TRANSFORMED_DATA_FILE
        },
    )

    load_task = PythonOperator(
        task_id='load_crash_data',
        python_callable=load_to_postgres,
        op_kwargs={
            'file_path': WORK_DIR+TRANSFORMED_DATA_FILE,
        },
    )
    
    visualise = PythonOperator(
        task_id='visualise',
        python_callable=visualise,
        op_kwargs={
            'file_path': WORK_DIR+TRANSFORMED_DATA_FILE,
            'output_file':WORK_DIR+GRAPH_HTML
        },
    )

    [extract_crashes_task, extract_moon_phases_task] >> transform_task >> [load_task,visualise]
