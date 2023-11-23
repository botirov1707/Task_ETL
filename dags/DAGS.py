from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import io



psql_user = 'postgres'
psql_pass = '12345'
psql_host = 'host.docker.internal'
psql_port = '5432'
psql_db = 'Task'
truncate = '1'

data_url = 'https://drive.google.com/uc?export=download&id=1vbHELLhLzr5VaDiaeesDQ6BRsEoFd9lK'

engine = create_engine(f'postgresql://{psql_user}:{psql_pass}@{psql_host}:{psql_port}/{psql_db}', pool_recycle=3600,
                       pool_pre_ping=True, echo=True)


def extract_data():
    res = requests.get(data_url)
    data = res.json()
    print(len(data['data']))
    return pd.DataFrame(data['data']).to_csv(sep=';', index=True)


def convert_data_types(data: str, **kwargs):
    df = pd.read_csv(io.StringIO(data), sep=';', index_col=0)

    df = df.mask((df.eq('')), other=pd.NA)  # replace empty string with NaN

    df['click_timestamp'] = df['click_timestamp'].astype('int64')
    df['tracking_id'] = df['tracking_id'].astype('int64')
    df['publisher_id'] = df['publisher_id'].astype('int64')
    df['is_bot'] = df['is_bot'].replace({'true': True, 'false': False})

    return df


def split_tables(df: pd.DataFrame):
    publishers = df[["publisher_name","publisher_id"]].copy()
    publishers.drop_duplicates(inplace=True)
    publishers.set_index('publisher_id', inplace=True)
    publishers.name = 'publisher'

    # -------------------------------
    trackers = df[["tracker_name","tracking_id"]].copy()
    trackers.drop_duplicates(inplace = True)
    trackers.set_index('tracking_id',inplace = True)
    trackers.name = 'trackers'

    # -------------------------------

    clicks = df[[
        "click_timestamp",
        "click_ipv6",
        "click_url_parameters",
        "click_id",
        "click_user_agent",
        "publisher_id",  # FK to publisher
        "tracking_id",
    ]].copy()
    #clicks.dropna(inplace=True)
    #clicks = clicks[clicks['click_idx'].notna()]
    clicks['device_id'] = df.index
    clicks.set_index('device_id', inplace=True)
   # clicks = clicks.assign(click_idx=clicks.index).set_index('click_idx')
   # clicks = clicks.assign(device_id=clicks.index).set_index('device_id')
    clicks.name = 'clicks'

    # -------------------------------

    devices = df[[
        "ios_ifa",
        "ios_ifv",
        "android_id",
        "google_aid",
        "os_name",
        "os_version",
        "device_manufacturer",
        "device_model",
        "device_type",
        "is_bot",
        "country_iso_code",
        "city",
    ]].copy()

    devices['device_id'] = df.index
    devices.set_index('device_id', inplace=True)
    devices.name = 'devices'



    return publishers, trackers, clicks, devices


def load_data(df_json: pd.DataFrame, table_name: str, **kwargs):
    global engine
    with engine.connect() as conn:
        df = pd.read_csv(io.StringIO(df_json), sep=';', index_col=0)

        df.to_sql(table_name, con=conn, if_exists='append', index=True)
        print(f'Inserted {df.shape[0]} rows into {table_name}')


def truncate_table(conn: object, table_name: str):
    conn.execute(
        text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"),
    )


def truncate_data(**kwargs):
    global engine
    with engine.connect() as conn:
        tables = ['publishers', 'trackers', 'clicks', 'devices']
        for table in tables:
            truncate_table(conn, table)


def transform(data: str, **kwargs):
    convert_data = convert_data_types(data)


    publisher, trackers, clicks, devices = split_tables(convert_data)

    kwargs['ti'].xcom_push(key='publishers', value=publisher.to_csv(index=True, sep=';'))

    kwargs['ti'].xcom_push(key='trackers', value=trackers.to_csv(index=True, sep=';'))

    kwargs['ti'].xcom_push(key='clicks', value=clicks.to_csv(index=True, sep=';'))

    kwargs['ti'].xcom_push(key='devices', value=devices.to_csv(index=True, sep=';'))

    return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'retry_delay': timedelta(minutes=5),
}

# Configuration for each table
table_config = {

    'devices':     {'task_id': 'device_table', 'key': 'devices'},
    'publishers':  {'task_id': 'publisher_table',   'key': 'publishers'},
    'trackers':    {'task_id': 'trackers_table',    'key': 'trackers'},
    'clicks':      {'task_id': 'clicks_table',       'key': 'clicks'},
}

with DAG('Task_ETL', default_args=default_args, schedule='@once', catchup=False) as dag:
    download = PythonOperator(
        task_id='download',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_kwargs={'data': '{{ ti.xcom_pull(task_ids="download") }}'},
    )

    truncate_tables = PythonOperator(
        task_id='truncate_tables',
        python_callable=truncate_data,
    )

    # Dynamically cr eat e tasks for each table
    for table_name, config in table_config.items():
        insert_task = PythonOperator(
            task_id=config['task_id'],
            python_callable=load_data,
            op_kwargs={
                'df_json': f'{{{{ ti.xcom_pull(task_ids="transform", key="{config["key"]}") }}}}',
                'table_name': table_name,
            },
        )

        # Set dependencies
        download >> transform >> truncate_tables >> insert_task
