from airflow import DAG

from datetime import datetime
import logging

from plugins.redshift_summary import RedshiftSummaryOperator
import os

dag = DAG(
    dag_id = 'week5_3',
    start_date = datetime(2020,11,8), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 2 * * *',  # 적당히 조절
)

user_summary = RedshiftSummaryOperator(
    task_id = 'user_summary',
    schema = 'sdrlurker',
    table = 'user_summary',
    redshift_conn_id = 'redshift_dev_db',
    input_check = [{
        'sql': 'SELECT COUNT(1) FROM raw_data.user_session_channel',
        'count': 101000},],
    main_sql = """SELECT c.channelname, usc.sessionid, usc.userid, sts.ts, st.refunded, st.amount FROM raw_data.channel AS c
LEFT JOIN raw_data.user_session_channel AS usc ON c.channelname = usc.channel
LEFT JOIN raw_data.session_timestamp AS sts ON usc.sessionid = sts.sessionid
LEFT JOIN raw_data.session_transaction AS st ON usc.sessionid = st.sessionid""",
    output_check = [{
        'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
        'count': 101000},],
    overwrite = True,
    dag = dag)

user_summary
