from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Параметры
TABLE_NAME = "dm.dm_f101_round_f"
CSV_FILE_PATH = "/tmp/dm_f101_round_f.csv"


# Логирование ETL-статуса
def log_etl_status(task_id, status, records_processed=0, start_time=None, end_time=None):
    postgres_hook = PostgresHook(postgres_conn_id='postgres-db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    query = """
    INSERT INTO LOGS.ETL_LOG (dag_id, task_id, start_time, end_time, status, records_processed)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        'export_dm_f101_round_f_csv',
        task_id,
        start_time,
        end_time,
        status,
        records_processed,
    ))

    conn.commit()
    cursor.close()


# Функция для выгрузки данных в CSV
def export_to_csv():
    start_time = datetime.now()
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres-db')
        sql_query = f"SELECT * FROM {TABLE_NAME}"
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]

        row_count = 0
        with open(CSV_FILE_PATH, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # Первая строка - заголовки
            for row in cursor.fetchall():
                writer.writerow(row)
                row_count += 1

        end_time = datetime.now()
        log_etl_status('export_to_csv', 'SUCCESS', row_count, start_time, end_time)
        print(f"Данные успешно выгружены в {CSV_FILE_PATH}. Обработано строк: {row_count}")

    except Exception as e:
        end_time = datetime.now()
        log_etl_status('export_to_csv', 'FAILED', 0, start_time, end_time)
        raise e


# Создание DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'export_dm_f101_round_f_csv',
        default_args=default_args,
        description='Выгрузка данных витрины dm_f101_round_f в CSV',
        schedule_interval=None,  # Выполняется вручную
        start_date=days_ago(1),
        tags=['peskov'],
        catchup=False,
) as dag:
    export_task = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv
    )
