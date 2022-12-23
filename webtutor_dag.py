import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import numpy as np
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.vertica_operator import VerticaOperator


source_con = BaseHook.get_connection('cl02sql\inst02sql')
source_username = source_con.login
source_password = quote(source_con.password)
source_host = source_con.host
source_db = 'datatutor'
eng_str = fr'mssql://{source_username}:{source_password}@{source_host}/{source_db}?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
source_engine =sa.create_engine(eng_str)

dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)

subdivision = """
                WITH 
                sq1 AS (
                    SELECT 	id
                            ,created
                            ,modified
                            ,data.value('(/subdivision/id)[1]', 'bigint')                                                           AS xml_id
                            ,data.value('(/subdivision/code)[1]', 'varchar(100)')                                                   AS code
                            ,data.value('(/subdivision/name)[1]', 'varchar(500)')                                                   AS name
                            ,data.value('(/subdivision/org_id)[1]', 'bigint')                                                       AS org_id
                            ,data.value('(/subdivision/is_disbanded)[1]', 'bit')                                                    AS is_disbanded
                            ,data.value('(/subdivision/place_id)[1]', 'bigint')                                                     AS place_id
                            ,data.value('(/subdivision/region_id)[1]', 'bigint')                                                    AS region_id
                            ,data.value('(/subdivision/formed_date)[1]', 'datetime')                                                AS formed_date
                            ,data.value('(/subdivision/is_faculty)[1]', 'bit')                                                      AS is_faculty
                            ,data.value('(/subdivision/comment)[1]', 'varchar(1000)')                                               AS comment
                            ,data.value('(/subdivision/doc_info/creation/user_id)[1]','bigint')                                     AS creator_id
                            ,data.value('(/subdivision/doc_info/creation/date)[1]', 'datetime')                                     AS xml_creation_date
                            ,data.value('(/subdivision/doc_info/modification/user_id)[1]','bigint')                                 AS modificator_id
                            ,data.value('(/subdivision/doc_info/modification/date)[1]', 'datetime')                                 AS xml_modification_date
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="type_dc"]/value)[1]', 'varchar(100)')         AS type_dc
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="site_diler"]/value)[1]', 'varchar(500)')      AS site_diler
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="tel"]/value)[1]', 'varchar(500)')             AS tel
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="street"]/value)[1]', 'varchar(1000)')         AS street
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="id_stoyanki"]/value)[1]', 'varchar(500)')     AS id_stoyanki
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="f_20u4"]/value)[1]', 'varchar(500)')          AS f_20u4
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="id_plowadki"]/value)[1]', 'varchar(500)')     AS id_plowadki
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="email"]/value)[1]', 'varchar(500)')           AS email
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="gps"]/value)[1]', 'varchar(500)')             AS gps
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="skidki"]/value)[1]', 'varchar(500)')          AS skidki
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="doc_obor"]/value)[1]', 'varchar(500)')        AS doc_obor
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="f_4xwo"]/value)[1]', 'varchar(500)')          AS f_4xwo
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="f_nliz"]/value)[1]', 'varchar(500)')          AS f_nliz
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="f_so4s"]/value)[1]', 'varchar(500)')          AS f_so4s
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="f_postpril1"]/value)[1]', 'varchar(500)')     AS f_postpril1
                            ,data.value('(/subdivision/custom_elems/custom_elem[name="f_dilerskidki"]/value)[1]', 'varchar(500)')   AS f_dilerskidki
	                FROM    subdivision
	                WHERE data.value('(/subdivision/doc_info/creation/date)[1]', 'datetime')  > CAST('{0}' AS DATETIME2)
	                        OR data.value('(/subdivision/doc_info/modification/date)[1]', 'datetime')  > CAST('{0}' AS DATETIME2)
                ),
                sq2 AS(
                	SELECT  id
                            ,Table1.field.query('name').value('.', 'varchar(500)') 													AS plan_name
                            ,Table1.field.value('(./value)[1]', 'varchar(500)') 													AS plan_value
	                FROM    subdivision
	                        CROSS APPLY subdivision.data.nodes('(/subdivision/custom_elems/custom_elem)') AS Table1(field)
	                WHERE data.value('(/subdivision/doc_info/creation/date)[1]', 'datetime')  > CAST('{0}' AS DATETIME2)
	                        OR data.value('(/subdivision/doc_info/modification/date)[1]', 'datetime')  > CAST('{0}' AS DATETIME2)
                ),
                sq3 AS(
                	SELECT *
                	FROM sq2
                	WHERE plan_name LIKE '%plan%'
                )
                SELECT sq1.*, sq3.plan_name, sq3.plan_value
                FROM sq1
                LEFT JOIN sq3
                	ON sq1.id = sq3.id;

                """


def extract(data_type):

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

    command = globals()[data_type].format(ts_from)

    return pd.read_sql_query(
        command,
        source_engine
    )

def transform(data, data_type):

    if not data.empty and data_type == 'subdivision':
        data['xml_id'] = data['xml_id'].fillna(0).astype(np.int64)
        data['org_id'] = data['org_id'].fillna(0).astype(np.int64)
        data['place_id'] = data['place_id'].fillna(0).astype(np.int64)
        data['region_id'] = data['region_id'].fillna(0).astype(np.int64)
        data['creator_id'] = data['creator_id'].fillna(0).astype(np.int64)
        data['modificator_id'] = data['modificator_id'].fillna(0).astype(np.int64)
        data['id_stoyanki'] = data['id_stoyanki'].fillna(0).astype(np.int64)
        data['id_plowadki'] = data['id_plowadki'].fillna(0).astype(np.int64)
    return data

def load(data, data_type):

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

        max_update_ts = max(
            max(data['xml_creation_date']),
            max(data['xml_modification_date']),
        )

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

    data_in_source = pd.read_sql_query(
        f"""
        SELECT COUNT(DISTINCT id) FROM {data_type};
        """,
        source_engine,
    ).values[0][0]

    data_in_dwh = pd.read_sql_query(
        f"""
        SELECT COUNT(DISTINCT id) FROM sttgaz.stage_webtutor_{data_type}
        """,
        dwh_engine,
    ).values[0][0]
    if data_in_source != data_in_dwh:
        raise Exception(
            f'Количество уникальных id в источнике и хранилище не совпадают:{data_in_source} != {data_in_dwh}'
        )

def etl(data_type):

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
        )
        for data_type in data_types:
            tasks.append(
                PythonOperator(
                    task_id=f'Получение_данных_{data_type}',
                    python_callable=etl,
                    op_kwargs={'data_type': data_type},
                )
            )
        
        tasks
    
    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> end
