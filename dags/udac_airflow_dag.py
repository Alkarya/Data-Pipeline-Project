from datetime import datetime, timedelta
import os
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
    'start_date': datetime(2024, 1, 12),
    'retries': 3,
    'retry_delay': 300,
    'catchup': True,
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
    select_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    select_sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    select_sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    select_sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    select_sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    operations = [
        {
            'query' : 'SELECT COUNT(*) FROM public.artists;',
            'check' : 'greater',
            'value' : '0'
        },
        {
            'query' : 'SELECT COUNT(*) FROM public.songplays;',
            'check' : 'greater',
            'value' : '0'
        },
        {
            'query' : 'SELECT COUNT(*) FROM public.songs;',
            'check' : 'greater',
            'value' : '0'
        },
        {
            'query' : 'SELECT COUNT(*) FROM public.time;',
            'check' : 'greater',
            'value' : '0'
        },
        {
            'query' : 'SELECT COUNT(*) FROM public.users;',
            'check' : 'greater',
            'value' : '0'
        },
        {
            'query' : 'SELECT COUNT(*) FROM public.artists WHERE artistid IS NULL;',
            'check' : 'equal',
            'value' : '0'
        },
        {
            'query' : 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL;',
            'check' : 'equal',
            'value' : '0'
        },
        {
            'query' : 'SELECT COUNT(*) FROM public.songs WHERE songid IS NULL;',
            'check' : 'equal',
            'value' : '0'
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


## DAG dependencies

# 1. Staging phase 
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# 2. Fact phase
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# 3. Dimension phase 
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# 4. Data Quality phase
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
