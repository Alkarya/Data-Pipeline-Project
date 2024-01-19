from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTablesOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Alkarya',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_airflow_dag',
          default_args=default_args,
          description='Loads and transforms data in Redshift',
          schedule_interval='0 * * * *'
        )

#No longer a dummy operator, used to create tables if they do not exist
start_operator = CreateTablesOperator(
    task_id='Begin_execution',  
    dag=dag,
    redshift_conn_id='redshift',
    sql_file='/home/workspace/airflow/create_tables.sql'    
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    table='staging_events',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    
    s3_bucket='udacity-dend',
    s3_prefix='song_data',
    table='staging_songs',
    copy_options="FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


## DAG dependencies

# 1. Staging phase 
start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]

# 2. Fact phase
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table

# 3. Dimension phase 
load_songplays_table >> [load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table]

# 4. Data Quality phase
[load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks

# 5. End Phase
run_quality_checks >> end_operator
