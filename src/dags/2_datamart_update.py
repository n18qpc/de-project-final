from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime
from vertica_python import connect
from airflow.hooks.dbapi_hook import DbApiHook
import os

log = logging.getLogger(__name__)


class VerticaHook(DbApiHook):
    conn_name_attr = 'vertica_conn_id'
    default_conn_name = 'vertica_connection'
    supports_autocommit = True

    def get_conn(self):
        conn = self.get_connection(self.vertica_conn_id)
        conn_config = {"user": conn.login, "password": conn.password or '', "database": conn.schema, "autocommit": True,
                       "host": conn.host or 'localhost'}
        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)
        conn = connect(**conn_config)
        return conn
    

def get_execution_date(**context) -> str:
    execution_date = os.environ["AIRFLOW_CTX_EXECUTION_DATE"]
    log.info(f'Текущая execution_date: {execution_date}')
    return str(execution_date).split('T')[0]

def update_mart(schema: str, table_name: str, vertica_connection: object):
    chosen_day = get_execution_date()
    log.info(msg=f'Начало загрузки данных в Vertica: {datetime.now().strftime("%Y-%m-%d %H:%M:%s")}')
    with vertica_connection:
        cur = vertica_connection.cursor()
        sql_script = open('/src/sql/update_mart.sql', 'r').read()
        sql_script.replace('chosen_day', chosen_day)
        cur.execute(sql_script)
        log.info(
            f"В таблицу {schema}.{table_name}: внесено {cur.fetchall()[0][0]} записей. "
            f"Время завершения загрузки: {datetime.now().strftime('%Y-%m-%d %H:%M:%s')}")

def check_schema(schema: str, vertica_conn: object) -> None:
    with vertica_conn as conn:
        cur = conn.cursor()
        script_name = f'/src/sql/{schema}.sql'
        cur.execute(open(script_name, 'r').read())
    log.info(f'Проверка схемы {schema} завершена успешно') 

dag = DAG(
    schedule='0 8 * * *',
    dag_id='datamart_update',
    start_date=datetime(2023, 6, 2),
    end_date=datetime(2023, 7, 1),
    catchup=False,
    tags=['dwh', 'vertica']
)

check_dwh = PythonOperator(task_id='check_dwh',
                           python_callable=check_schema,
                           op_kwargs={'schema': 'EGOROVMAKSIM14YANDEXRU__DWH',
                                      'vertica_connection': VerticaHook().get_conn()},
                           dag=dag)

update_datamart = PythonOperator(task_id='update_dwh',
                            python_callable=update_mart,
                            op_kwargs={'schema': 'EGOROVMAKSIM14YANDEXRU__DWH', 'table_name': "global_metrics",
                                       'vertica_connection': VerticaHook().get_conn()},
                            dag=dag)

check_dwh >> update_datamart