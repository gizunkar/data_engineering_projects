from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow import configuration
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
import logging

from plugins.ssh_spark_submit_operator import SSHSparkSubmitOperator
from operators.livy_operator import SparkLivyBatchOperator
from operators import (StageToRedshiftOperator, DataQualityOperator)


default_args = {
    'owner': 'gizem',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dags_folder = configuration.conf.get('core', 'dags_folder')
dag = DAG('yelp_dwh_pipeline',
          default_args=default_args,
          catchup=False,
          description='Store source data as files',
          schedule_interval='0 0 * * *',
          template_searchpath=dags_folder + "/yelp_data_eng/templates",
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# creating fact and dimensions
state_dim_task = SSHSparkSubmitOperator(
    task_id='state_dim_task',
    dag=dag,
    default_args=default_args,
    ssh_conn_id='ssh_conn_dataeng_spark',
    conn_id='spark_conn_dataeng_spark',
    name='sparkyelp',
    application='s3://udend-capstone-yelp-code/yelp_dwh.py',
    application_args=[Variable.get("s3_input_path"), Variable.get("s3_output_path"), "state_dim"],

)
category_dim_task = SSHSparkSubmitOperator(
    task_id='category_dim_task',
    dag=dag,
    default_args=default_args,
    ssh_conn_id='ssh_conn_dataeng_spark',
    conn_id='spark_conn_dataeng_spark',
    name='sparkyelp',
    application='s3://udend-capstone-yelp-code/yelp_dwh.py',
    application_args=[Variable.get("s3_input_path"), Variable.get("s3_output_path"), "category_dim"],

)

ambience_dim_task = SSHSparkSubmitOperator(
    task_id='ambience_dim_task',
    dag=dag,
    default_args=default_args,
    ssh_conn_id='ssh_conn_dataeng_spark',
    conn_id='spark_conn_dataeng_spark',
    name='sparkyelp',
    application='s3://udend-capstone-yelp-code/yelp_dwh.py',
    application_args=[Variable.get("s3_input_path"), Variable.get("s3_output_path"), "ambience_dim"],

)

user_dim_task = SSHSparkSubmitOperator(
    task_id='user_dim_task',
    dag=dag,
    default_args=default_args,
    ssh_conn_id='ssh_conn_dataeng_spark',
    conn_id='spark_conn_dataeng_spark',
    name='sparkyelp',
    application='s3://udend-capstone-yelp-code/yelp_dwh.py',
    application_args=[Variable.get("s3_input_path"), Variable.get("s3_output_path"), "user_dim"],

)

business_reviews_fact_task = SSHSparkSubmitOperator(
    task_id='business_reviews_fact_task',
    dag=dag,
    default_args=default_args,
    ssh_conn_id='ssh_conn_dataeng_spark',
    conn_id='spark_conn_dataeng_spark',
    name='sparkyelp',
    application='s3://udend-capstone-yelp-code/yelp_dwh.py',
    application_args=[Variable.get("s3_input_path"), Variable.get("s3_output_path"), "business_reviews_fact"],

)


# Create dimension tables in Redshift
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

# #  Load dimensions
copy_category_toRedshift = StageToRedshiftOperator(
    task_id='copy_category_toRedshift',
    dag=dag,
    table="category",
    s3_bucket=Variable.get("yelp_bucket"),
    s3_key='output/category.json',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    is_json=True
)
#
copy_ambience_toRedshift = StageToRedshiftOperator(
    task_id='copy_ambience_toRedshift',
    dag=dag,
    table="ambience",
    s3_bucket=Variable.get("yelp_bucket"),
    s3_key='output/ambience.json',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    is_json=True
)

copy_state_toRedshift = StageToRedshiftOperator(
    task_id='copy_state_toRedshift',
    dag=dag,
    table="state",
    s3_bucket=Variable.get("yelp_bucket"),
    s3_key='output/state.json',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    is_json=True
)

copy_users_toRedshift = StageToRedshiftOperator(
    task_id='copy_user_toRedshift',
    dag=dag,
    table="users",
    s3_bucket=Variable.get("yelp_bucket"),
    s3_key='output/user.json',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    is_json=True
)

copy_business_reviews_toRedshift = StageToRedshiftOperator(
    task_id='copy_business_reviews_toRedshift',
    dag=dag,
    table="business_reviews",
    s3_bucket=Variable.get("yelp_bucket"),
    s3_key='output/business_reviews.json',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    is_json=True
)

data_quality_task = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="business_reviews",
)

end_operator = DummyOperator(task_id='end_of_execution', dag=dag)

copy_start_operator = DummyOperator(task_id='copy_start_operator', dag=dag)

start_operator >> state_dim_task >> business_reviews_fact_task
start_operator >> category_dim_task >> business_reviews_fact_task
start_operator >> ambience_dim_task >> business_reviews_fact_task
start_operator >> user_dim_task >> business_reviews_fact_task

# business_reviews_fact_task >> delete_from_s3_task
business_reviews_fact_task >> create_tables_task

create_tables_task >> copy_start_operator
# delete_from_s3_task >> copy_start_operator
#

copy_start_operator >> copy_category_toRedshift >> end_operator
copy_start_operator >> copy_ambience_toRedshift >> end_operator
copy_start_operator >> copy_state_toRedshift >> end_operator
copy_start_operator >> copy_users_toRedshift >> end_operator
copy_start_operator >> copy_business_reviews_toRedshift >> data_quality_task >>end_operator
