from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow import configuration
from airflow.models import Variable
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
""" This DAG is developed to take extractions from S3 into Redshift and implement a Star schema in Redshift. """

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 12,2 ),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('sparkify_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 1,15 * *',
          template_searchpath="/home/workspace/airflow/",
          max_active_runs=1,
          catchup=True
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task= PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_bucket=Variable.get("s3_bucket"),
    s3_key=Variable.get("s3_log_prefix"),
    jsonpath='s3://udacity-dend/log_json_path.json',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    is_json = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_bucket=Variable.get("s3_bucket"),
    s3_key=Variable.get("s3_song_prefix"),
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    is_json = True
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    table="songplays",

)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.user_table_insert,
    table="users",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert,
    table="songs",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    table="artists",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    table="time",
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift>> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table>> load_song_dimension_table >> run_quality_checks
load_songplays_table>> load_user_dimension_table >> run_quality_checks
load_songplays_table>> load_artist_dimension_table >> run_quality_checks
load_songplays_table>> load_time_dimension_table >> run_quality_checks

run_quality_checks>> end_operator

