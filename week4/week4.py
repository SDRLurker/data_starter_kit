from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import psycopg2
import requests
import config

dag = DAG(
    dag_id = 'week4',
    start_date = datetime(2020,10,31),
    schedule_interval = '0 2 * * *')

def get_Redshift_connection():
    host = config.host
    redshift_user = config.redshift_user
    redshift_pass = config.redshift_pass
    port = config.port
    dbname = config.dbname
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=False)
    return conn

def extract(url, **kwargs):
    f = requests.get(url)
    return f.text

def transform(**kwargs):
    print(kwargs)
    ti = kwargs['ti']
    text = ti.xcom_pull(task_ids='extract')
    lines = text.split("\n")
    return lines

def load(**kwargs):
    print(kwargs)
    ti = kwargs['ti']
    lines = ti.xcom_pull(task_ids='transform')
    def execute(sql):
        print(sql)
        cur.execute(sql)
    with get_Redshift_connection() as conn:
        try:
            cur = conn.cursor()
            execute("BEGIN;DELETE FROM sdrlurker.name_gender;")
            for r in lines:
                if r != '' and not r.startswith('name'):
                    (name, gender) = r.split(",")
                    print(name, "-", gender)
                    sql = "INSERT INTO sdrlurker.name_gender VALUES ('{name}', '{gender}')".format(name=name, gender=gender)
                    execute(sql)
            execute("END;")
        except:
            print("<<< rollback >>>")
            conn.rollback()
        finally:
            cur.close()

extract = PythonOperator(
    task_id = 'extract',
    #python_callable param points to the function you want to run 
    python_callable = extract,
    op_kwargs={"url":"https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"},
    #dag param points to the DAG that this task is a part of
    dag = dag)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    provide_context = True,
    dag = dag)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    provide_context = True,
    dag = dag)

#Assign the order of the tasks in our DAG
extract >> transform >> load