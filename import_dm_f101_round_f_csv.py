from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Параметры
COPY_TABLE_NAME = "DM.dm_f101_round_f"
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
        'import_dm_f101_round_f_csv',
        task_id,
        start_time,
        end_time,
        status,
        records_processed,
    ))

    conn.commit()
    cursor.close()


# Функция для загрузки данных из CSV
def import_from_csv():
    start_time = datetime.now()
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres-db')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Очистка таблицы перед загрузкой
        cursor.execute(f"TRUNCATE TABLE {COPY_TABLE_NAME}")

        # Загрузка данных из CSV
        row_count = 0
        with open(CSV_FILE_PATH, 'r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Пропустить заголовки
            for row in reader:
                cursor.execute(
                    f"INSERT INTO {COPY_TABLE_NAME} ({', '.join(headers)}) VALUES ({', '.join(['%s'] * len(row))})",
                    row
                )
                row_count += 1

        conn.commit()
        end_time = datetime.now()
        log_etl_status('import_from_csv', 'SUCCESS', row_count, start_time, end_time)
        print(f"Данные успешно загружены в {COPY_TABLE_NAME}. Обработано строк: {row_count}")

    except Exception as e:
        end_time = datetime.now()
        log_etl_status('import_from_csv', 'FAILED', 0, start_time, end_time)
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
        'import_dm_f101_round_f_csv',
        default_args=default_args,
        description='Импорт данных из CSV в таблицу dm_f101_round_f',
        schedule_interval=None,  # Выполняется вручную
        start_date=days_ago(1),
        tags=['peskov'],
        catchup=False,
) as dag:
    import_task = PythonOperator(
        task_id='import_from_csv',
        python_callable=import_from_csv
    )
