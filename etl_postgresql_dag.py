from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import time

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
        'etl_postgresql_dag',
        task_id,
        start_time,
        end_time,
        status,
        records_processed,
    ))

    conn.commit()
    cursor.close()


# Общая функция для загрузки данных в любую таблицу
def load_table(file_path, table_name, conflict_columns, encoding='utf-8', **kwargs):
    if table_name == 'DS.MD_CURRENCY_D':
        encoding = 'cp1252'

    postgres_hook = PostgresHook(postgres_conn_id='postgres-db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Логируем начало загрузки
    start_time = datetime.now()
    log_etl_status(f'load_{table_name}', 'start', 0, start_time=start_time)

    # Пауза на 5 секунд
    time.sleep(5)

    try:
        # Чтение данных из файла
        df = pd.read_csv(file_path, delimiter=";", encoding=encoding)

        if table_name == 'DS.MD_CURRENCY_D':
            # Преобразуем значения в строки и обрабатываем NaN корректно
            df['CURRENCY_CODE'] = df['CURRENCY_CODE'].fillna('').astype(str).apply(
                lambda x: x[:3] if isinstance(x, str) else '')
            df['CODE_ISO_CHAR'] = df['CODE_ISO_CHAR'].fillna('').astype(str).apply(
                lambda x: x[:3] if isinstance(x, str) else '')

        # Преобразование столбцов с датами
        if table_name == 'DS.FT_BALANCE_F':
            df['ON_DATE'] = pd.to_datetime(df['ON_DATE'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')
        if table_name == 'DS.FT_POSTING_F':
            df['OPER_DATE'] = pd.to_datetime(df['OPER_DATE'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')

        # Загрузка данных
        records_processed = 0
        for _, row in df.iterrows():
            query = f"""
            INSERT INTO {table_name} ({', '.join(df.columns)})
            VALUES ({', '.join(['%s'] * len(df.columns))})
            ON CONFLICT ({', '.join(conflict_columns)})
            DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in conflict_columns])};
            """
            cursor.execute(query, tuple(row))
            records_processed += 1

        conn.commit()

        # Логируем успешное завершение
        end_time = datetime.now()
        log_etl_status(f'load_{table_name}', 'success', records_processed, start_time=start_time, end_time=end_time)

    except Exception as e:
        conn.rollback()

        # Логируем ошибку
        end_time = datetime.now()
        log_etl_status(f'load_{table_name}', 'failure', 0, start_time=start_time, end_time=end_time)
        raise
    finally:
        cursor.close()
        conn.close()


# Дефолтные аргументы для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG
with DAG(
    'etl_postgresql_dag',
    default_args=default_args,
    description='ETL-процесс для первой части проектной работы',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['peskov'],
    catchup=False,
) as dag:

    # Определение задач
    tasks = []

    table_configs = [
        {
            "file_path": "/lab_af/ft_balance_f.csv",
            "table_name": "DS.FT_BALANCE_F",
            "conflict_columns": ["on_date", "account_rk"]
        },
        {
            "file_path": "/lab_af/ft_posting_f.csv",
            "table_name": "DS.FT_POSTING_F",
            "conflict_columns": ["oper_date", "credit_account_rk", "debet_account_rk"]
        },
        {
            "file_path": "/lab_af/md_account_d.csv",
            "table_name": "DS.MD_ACCOUNT_D",
            "conflict_columns": ["data_actual_date", "account_rk"]
        },
        {
            "file_path": "/lab_af/md_currency_d.csv",
            "table_name": "DS.MD_CURRENCY_D",
            "conflict_columns": ["currency_rk"]
        },
        {
            "file_path": "/lab_af/md_exchange_rate_d.csv",
            "table_name": "DS.MD_EXCHANGE_RATE_D",
            "conflict_columns": ["data_actual_date", "currency_rk"]
        },
        {
            "file_path": "/lab_af/md_ledger_account_s.csv",
            "table_name": "DS.MD_LEDGER_ACCOUNT_S",
            "conflict_columns": ["ledger_account", "start_date"]
        },
    ]

    # Создание задач для каждой таблицы
    for config in table_configs:
        task = PythonOperator(
            task_id=f"load_{config['table_name'].replace('.', '_').lower()}",
            python_callable=load_table,
            op_kwargs=config,
        )
        tasks.append(task)

    # Последовательность задач
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
