import cx_Oracle
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
import numpy as np
import pytz
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


tz = pytz.timezone('Asia/Jakarta')


def localize_utc_tz(d):
    return tz.fromutc(d)


# define connection
ora_con = cx_Oracle.connect(
    "DATA_NIKEL", "thisisnottherealpassword", "192.168.1.100:1521/PROD", encoding="UTF-8"
)

credentials = service_account.Credentials.from_service_account_file(
    '/home/nikel/key/nikelcredential.json',
)

bq_client = bigquery.Client(credentials=credentials)


initial_query = '''
SELECT /*+ parallel(2) */
   a.loan_id,
   a.loan_amount,
   a.borrower_id,
   a.status,
   a.partner,
   b.partner_name,
   a.current_dpd,
   a.interest_rate,
   a.loan_term,
   a.created_date,
   a.updated_date,
FROM nikel_prod.loan_level a
INNER JOIN nikel_prod.partner_level b on a.partner = b.partner
WHERE EXTRACT(YEAR FROM created_date) = 2023
  AND EXTRACT(MONTH FROM created_date) = 6
'''

incr_query = '''
SELECT /*+ parallel(2) */
   a.loan_id,
   a.loan_amount,
   a.borrower_id,
   a.status,
   a.partner,
   b.partner_name,
   a.current_dpd,
   a.interest_rate,
   a.loan_term,
   a.created_date,
   a.updated_date,
FROM nikel_prod.loan_level a
INNER JOIN nikel_prod.partner_level b on a.partner = b.partner
WHERE created_date >= SYSDATE - INTERVAL '7' DAY
OR updated_date >= SYSDATE - INTERVAL '7' DAY
'''


def trunc_insert(query, ora_con, bq_table):
    df = pd.read_sql_query(query, con=ora_con)

    print('querry success')
    
    table_id = f'project_nikel.{bq_table}'
    trunc = f'''delete from {table_id} where created_date >= DATE_ADD(DATE(current_timestamp(), 'Asia/Jakarta'), INTERVAL -7 DAY'''
    trunc_job = bq_client.query(trunc)
    trunc_job.result()

    print("Truncate Success!")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", source_format='CSV'
    )

    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  
    job.result()
    print("Insert Success!")

    return "Job Finish!"


# Author
default_args = {
    'owner': 'Nikel',
    'depends_on_past': False,
    'email': ['dataengineer@nikel.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    #'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Schedule
with DAG(
    'dag_nikel_daily',
    default_args=default_args,
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 6, 12, tz="Asia/Jakarta"),
    catchup=False,
    tags=['nikel','test_nikel'],
    user_defined_filters={
        'localtz': localize_utc_tz,
    },
) as dag:
     
    stg1 = PythonOperator(
        task_id='nikel_daily',
        python_callable=trunc_insert,
        op_args=[stg_wholesale_initial, ora_con, 'db_nikel.raw_db'],
    )
    
    stg1
