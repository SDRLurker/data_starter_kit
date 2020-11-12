from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2

from plugins.postgres_to_s3_operator import PostgresToS3Operator
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator
import os

tables = [
    "customer_features",
    "customer_variants",
    "item_features",
    "customer_interactions"
]

create_dic = {
    "customer_features":
        """customer_id int,
           engagement_level varchar(16),
           gender varchar(16),
           primary key(customer_id)""",
    "customer_variants":
        """customer_id int,
           variant varchar(8),
           primary key(customer_id)""",
    "item_features":
        """item_id int,
           quality float,
           popularity varchar(16),
           price int,
           primary key(item_id)""",
    "customer_interactions":
        """customer_id int,
           datastamp date,
           item_id int,
           clicked int,
           purchased int,
           paidamount int,
           primary key(customer_id, datastamp, item_id, clicked, purchased)""",
}

# s3_bucket, local_dir
s3_bucket = Variable.get("s3_bucket")
local_dir = Variable.get("local_dir")
s3_key_prefix = Variable.get("s3_key_prefix")
schema = Variable.get("schema")
prev_task = None

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn()


def create_tables():
    with get_Redshift_connection() as conn:
        cur = conn.cursor()
        for table in tables:
            sql = "CREATE TABLE IF NOT EXISTS {schema}.{table}({fields});".format(schema=schema, 
                table=table, fields=create_dic.get(table))
            cur.execute(sql)
            conn.commit()
        cur.close()

dag = DAG(
    dag_id = 'week5_2',
    start_date = datetime(2020,11,8), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '@once',  # 적당히 조절
    max_active_runs = 1,
    concurrency=2,
    catchup=False
)

dir_path = os.path.dirname(os.path.realpath(__file__))
print(dir_path)
print(os.getcwd())

prev_task = PythonOperator(
    task_id = 'create_tables',
    python_callable = create_tables,
    dag = dag)

for table in tables:
    s3_key=s3_key_prefix+'/'+table+'.tsv'

    postgrestos3 = PostgresToS3Operator(
        table="public."+table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        data_dir=local_dir,
        dag=dag,
        task_id="Postgres_to_S3"+"_"+table
    )

    s3toredshift = S3ToRedshiftOperator(
        schema=schema,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options="delimiter '\\t' COMPUPDATE ON",
        aws_conn_id='aws_s3_default',
        task_id='S3_to_Redshift'+"_"+table,
        dag=dag
    )
    if prev_task is not None:
        prev_task >> postgrestos3 >> s3toredshift
    else:
        postgrestos3 >> s3toredshift
    prev_task = s3toredshift
