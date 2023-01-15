import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import TaskInstance


source_con = BaseHook.get_connection('cl02sql\inst02sql')
source_username = source_con.login
source_password = quote(source_con.password)
source_host = source_con.host
source_db = 'datatutor'
eng_str = fr'mssql://{source_username}:{source_password}@{source_host}/{source_db}?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
source_engine = sa.create_engine(eng_str)

dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)


def extract(data_type):
    """Извлечение данных из источника."""

    df = pd.read_sql_query(
        f"""
        SELECT MAX(CAST(workflow_settings AS TIMESTAMP))
        FROM sttgaz.stage_workflow_status
        WHERE workflow_key = 'stage_webtutor_{data_type}';
        """,
        dwh_engine,
    )

    ts_from = df.values[0][0]

    if not ts_from:
        ts_from = '2000-01-01 00:00:00.000'

    print('Запрос данных из БД Webtutor c датой изменения от:', ts_from)
    
    with open(fr'/home/da/airflow/dags/WebTutor_etl/stage_webtutor_{data_type}.sql', 'r') as f:
        command = f.read().format(data_type, ts_from)

    print(command)

    return pd.read_sql_query(
        command,
        source_engine,
    )

def transform(data, data_type):
    """Преобразование/трансформация данных (ЕСЛИ НУЖНО)."""

    return data

def load(data, data_type):
    """Загрузка данных в хранилище."""

    if not data.empty:

        print(data)
        
        ids_for_del = tuple(data['id'].values)

        if len(ids_for_del) == 1:
            query_part = f' = {ids_for_del[0]};'
        else:
            query_part = f' IN {ids_for_del};'

        pd.read_sql_query(
            f"""
            DELETE FROM sttgaz.stage_webtutor_{data_type}
            WHERE id
            """ + query_part,
            dwh_engine,
        )

        if data_type == 'subdivision':
            max_update_ts = max(
                max(data['xml_creation_date']),
                max(data['xml_modification_date']),
            )
        else:
            max_update_ts = max(data['modification_date'])

        data.to_sql(
            f'stage_webtutor_{data_type}',
            dwh_engine,
            schema = 'sttgaz',
            if_exists ='append',
            index = False,
        )

        pd.read_sql_query(
            f"""
            INSERT INTO sttgaz.stage_workflow_status
            (workflow_key,	workflow_settings)
            VALUES
            ('stage_webtutor_{data_type}', '{max_update_ts}');
            """,
            dwh_engine,
        )
    else:
        print('Нет новых данных для загрузки.')

def check(data_type):
    """Проверяем успешность загрузки."""

    source_query = f"""
                    SELECT COUNT(DISTINCT id) FROM {data_type}
                    """
    dwh_query = f"""
                SELECT COUNT(DISTINCT id) FROM sttgaz.stage_webtutor_{data_type}
                """

    data_in_source = pd.read_sql_query(
        source_query,
        source_engine,
    ).values[0][0]

    data_in_dwh = pd.read_sql_query(
        dwh_query,
        dwh_engine,
    ).values[0][0]
    
    if data_in_source != data_in_dwh:
        raise Exception(
            f'Количество уникальных id в источнике и хранилище не совпадают:{data_in_source} != {data_in_dwh}'
        )


def etl(data_type):
    """Запускаем ETL-процесс для заданного типа данных."""
    data = extract(data_type)
    data = transform(data, data_type)
    load(data, data_type)
    check(data_type)


#-------------- DAG -----------------

default_args = {
    'owner': 'Швейников Андрей',
    'email': ['shveynikovab@st.tech'],
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'WebTutor',
        default_args=default_args,
        description='Получение данных из WebTutor.',
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        tasks = []
        data_types = (
            'subdivision',
            'subdivisions',
            'orgs',
            'regions',
            'places',
            'positions',
            'position_commons',
        )
        for data_type in data_types:
            tasks.append(
                PythonOperator(
                    task_id=f'Получение_данных_{data_type}',
                    python_callable=etl,
                    op_kwargs={'data_type': data_type},
                )
            )

        collaborators = PythonOperator(
            task_id=f'Получение_данных_collaborators',
            python_callable=etl,
            op_kwargs={'data_type': 'collaborators'},
        )

        collaborators_ti = TaskInstance(collaborators, {{execution_date}})

        do_nothing = DummyOperator(task_id='Все_ОК')
        remove_table = DummyOperator(task_id='Пересоздаем таблицу')
        
        load_result_check = BranchPythonOperator(
            task_id='load_result_check',
            python_callable = (lambda status: 'do_nothing' if status == 'success' else 'remove_table')(collaborators_ti.ti.current_state()),
            trigger_rule = 'all_done',
        )

        collapse = DummyOperator(task_id='collapse', trigger_rule = 'none_failed')

        collaborators >> load_result_check >> [do_nothing, remove_table] >> collapse

        tasks.append(collapse)
        
        tasks

    with TaskGroup('Формирование_слоя_DDS') as data_to_dds:

        tables_1 = (
            'dds_webtutor_regions',
            'dds_webtutor_position_commons',
        )

        parallel_tasks_1 = []

        for table in tables_1:
            parallel_tasks_1.append(
                VerticaOperator(
                    task_id=f'update_{table}',
                    vertica_conn_id='vertica',
                    sql=f'{table}.sql',
                )
            )

        dds_webtutor_places = VerticaOperator(
            task_id='update_dds_webtutor_places',
            vertica_conn_id='vertica',
            sql='dds_webtutor_places.sql',
        )

        dds_webtutor_orgs = VerticaOperator(
            task_id='update_dds_webtutor_orgs',
            vertica_conn_id='vertica',
            sql='dds_webtutor_orgs.sql',
        )

        dds_webtutor_subdivision = VerticaOperator(
            task_id='update_dds_webtutor_subdivision',
            vertica_conn_id='vertica',
            sql='dds_webtutor_subdivision.sql',
        )

        dds_webtutor_positions = VerticaOperator(
            task_id='update_dds_webtutor_positions',
            vertica_conn_id='vertica',
            sql='dds_webtutor_positions.sql',
        )

        tables_2 = (
            'dds_webtutor_collaborators',
            'dds_webtutor_plans',
        )

        parallel_tasks_2 = []

        for table in tables_2:
            parallel_tasks_2.append(
                VerticaOperator(
                    task_id=f'update_{table}',
                    vertica_conn_id='vertica',
                    sql=f'{table}.sql',
                )
            )

        parallel_tasks_1 >> dds_webtutor_places >> dds_webtutor_orgs >> dds_webtutor_subdivision >> dds_webtutor_positions >> parallel_tasks_2

    with TaskGroup('Формирование_слоя_dm') as data_to_dm:

        dm_webtutor_personal_stat = VerticaOperator(
            task_id='update_dm_webtutor_personal_stat',
            vertica_conn_id='vertica',
            sql='dm_webtutor_personal_stat.sql',
        )

        dm_webtutor_personal_stat

    with TaskGroup('Проверка_данных') as data_check:

        check_1 = VerticaOperator(
            task_id='checking_for_accuracy_of_execution',
            vertica_conn_id='vertica',
            sql='checking_for_accuracy_of_execution.sql'
        )

        check_1
    
    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_check >> end
