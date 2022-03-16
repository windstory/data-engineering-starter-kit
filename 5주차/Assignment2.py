from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    # PostgresHook은  autocommit의 Default값이 false
    return hook.get_conn().cursor()

def etl_weather_forecast(**context):
    lat = context["params"]["latitude"]
    lon = context["params"]["longitude"]
    api_key = context["params"]["api_key"]
    url = context["params"]["url_format"].format(lat=lat, lon=lon,api_key=api_key)
    
    json_data = extract_json_data(url)
    data_list = transform(json_data)
    load(context["params"]["schema"], context["params"]["table"], data_list)

def extract_json_data(url):
    res = requests.get(url)
    return (res.json)

def transform(json_data):
    daily_list = []
    for daily in json_data['daily']:
        dt = datetime.fromtimestamp(daily["dt"]).strftime('%Y-%m-%d')
        temp = daily['temp']['day']
        min_temp = daily['temp']['min']
        max_temp = daily['temp']['max']
        daily_list.append([dt, temp, min_temp, max_temp])
    return daily_list

def load(schema, table, data_list):
    values = []
    for row in data_list:
        (dt, temp, min_temp, max_temp) = row
        values.append("('{}', {}, {}, {})".format(dt, temp, min_temp, max_temp))

    cur = get_Redshift_connection()
    insert_sql = """DELETE FROM {schema}.{table};INSERT INTO {schema}.{table} VALUES """\
        .format(schema=schema,table=table)\
         + ",".join(values)
    try:
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise

dag_weather_forecast = DAG(
    dag_id = 'weather_forecast',
    start_date = datetime(2022,3,8),
    schedule_interval = '0 1 * * *', 
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

etl_task = PythonOperator(
    task_id = 'perform_etl',
    python_callable = etl_weather_forecast,
    params = {
        'url_format':  Variable.get("open_weather_api_url_format"),
        'api_key': Variable.get("open_weather_api_key"),
        'latitude': 37.5326,
        'longitude': 127.024612,
        'schema': 'windstoryya',
        'table': 'weather_forecast'
    },
    provide_context=True,
    dag = dag_weather_forecast)