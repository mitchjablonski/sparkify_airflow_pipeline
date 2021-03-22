from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
import queries_file as SqlQueries
from dim_table_subdag import create_and_load_table_dag
from airflow.operators.subdag_operator import SubDagOperator



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup':False,
}
start_date = datetime(2019, 1, 12)
dag_name = 'sparkify_core_etl'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          start_date=start_date
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_stage_events = CreateTableOperator(task_id='events_creation', 
                                     dag=dag, 
                                     redshift_conn_id='redshift',
                                    create_sql=SqlQueries.create_staging_events_table,
                                                table='staging_events')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)

create_staging_songs_table = CreateTableOperator(task_id='create_staging_songs_table', 
                                     dag=dag, 
                                     redshift_conn_id='redshift',
                                    create_sql=SqlQueries.create_staging_songs_table,
                                                table='staging_songs')

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json="auto"
)

create_songplays_table = CreateTableOperator(task_id='create_songplays_table', 
                                            dag=dag, 
                                            redshift_conn_id='redshift',
                                            create_sql=SqlQueries.create_songplays_table,
                                            table='songplays')

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    query=SqlQueries.songplay_table_insert
)


user_dim_task_id = "user_dim_subdag"
users_subtask_dag = SubDagOperator(
        subdag=create_and_load_table_dag(
        dag_name,
        user_dim_task_id,
        redshift_conn_id="redshift",
        create_sql=SqlQueries.create_users_table,
        insert_sql=SqlQueries.user_table_insert,
        table='users',
        data_qual_query=SqlQueries.user_table_check,
        less_one_check=False,
        start_date=start_date,
    ),
    task_id=user_dim_task_id,
    dag=dag,
)

song_dim_task_id = "song_dim_subdag"
song_subtask_dag = SubDagOperator(
    subdag=create_and_load_table_dag(
        dag_name,
        song_dim_task_id,
        redshift_conn_id="redshift",
        create_sql=SqlQueries.create_songs_table,
        insert_sql=SqlQueries.song_table_insert,
        table='songs',
        data_qual_query=SqlQueries.song_table_check,
        less_one_check=False,
        start_date=start_date,
    ),
    task_id=song_dim_task_id,
    dag=dag,
)

artists_dim_task_id = "artists_dim_subdag"
artists_subtask_dag = SubDagOperator(
    subdag=create_and_load_table_dag(
        dag_name,
        artists_dim_task_id,
        redshift_conn_id="redshift",
        create_sql=SqlQueries.create_artists_table,
        insert_sql=SqlQueries.artist_table_insert,
        table='artists',
        data_qual_query=SqlQueries.artist_table_check,
        less_one_check=True,
        start_date=start_date,
    ),
    task_id=artists_dim_task_id,
    dag=dag,
)

time_dim_task_id = "time_dim_subdag"
time_subtask_dag = SubDagOperator(
    subdag=create_and_load_table_dag(
        dag_name,
        time_dim_task_id,
        "redshift",
        create_sql=SqlQueries.create_time_table,
        insert_sql=SqlQueries.time_table_insert,
        table='time',
        data_qual_query=SqlQueries.time_table_check,
        less_one_check=True,
        start_date=start_date,
    ),
    task_id=time_dim_task_id,
    dag=dag,
)

songplays_qual_check = DataQualityOperator(
    task_id='songplays_qual_check',
    dag=dag,
    redshift_conn_id="redshift",
    data_qual_query=SqlQueries.songplay_table_check,
    table="songplays",
    less_one_check=True,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_staging_songs_table
create_staging_songs_table >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table

start_operator >> create_tables_stage_events
create_tables_stage_events >> stage_events_to_redshift 
stage_events_to_redshift >> load_songplays_table

create_songplays_table >> load_songplays_table

load_songplays_table >> songplays_qual_check

load_songplays_table >> time_subtask_dag

stage_events_to_redshift >> users_subtask_dag

stage_songs_to_redshift >> song_subtask_dag

stage_songs_to_redshift >> artists_subtask_dag


songplays_qual_check >> end_operator
time_subtask_dag >> end_operator
users_subtask_dag >> end_operator
song_subtask_dag >> end_operator
artists_subtask_dag >> end_operator
