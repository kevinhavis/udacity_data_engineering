from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

# Configure dag based on:
#   The DAG does not have dependencies on past runs
#   On failure, the task are retried 3 times
#   Retries happen every 5 minutes
#   Catchup is turned off
#   Do not email on retry

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False,
    'email_on_failure': False
}

dag = DAG(
    'sparkify_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    target_table='staging_events',
    s3_bucket='s3://udacity-dend/log_data',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    target_table='staging_songs',
    s3_bucket='s3://udacity-dend/song_data',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='songplays',
    query=SqlQueries.songplay_table_insert,
    append_flag=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='users',
    query=SqlQueries.user_table_insert,
    append_flag=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='songs',
    query=SqlQueries.song_table_insert,
    append_flag=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='artists',
    query=SqlQueries.artist_table_insert,
    append_flag=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='time',
    query=SqlQueries.time_table_insert,
    append_flag=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    target_tables=['songplays', 'songs', 'artists', 'time', 'users']
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

(
    start_operator
    >> [stage_events_to_redshift, stage_songs_to_redshift]
    >> load_songplays_table
    >> [
        load_artist_dimension_table,
        load_song_dimension_table,
        load_time_dimension_table,
        load_user_dimension_table
    ]
    >> run_quality_checks
    >> end_operator
)
