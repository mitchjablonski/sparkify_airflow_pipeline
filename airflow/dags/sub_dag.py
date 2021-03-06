from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
def create_and_load_table_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        create_sql,
        insert_sql,
        table,
        truncate,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    create_users_table = CreateTableOperator(task_id=f'create_{table}_table', 
                                            dag=dag, 
                                            redshift_conn_id=redshift_conn_id,
                                            create_sql=create_sql,
                                            table=table)

    load_user_dimension_table = LoadDimensionOperator(
        task_id=f'Load_{table}_dim_table',
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        query=insert_sql,
        truncate=truncate
    )

    create_users_table >> load_user_dimension_table

    return dag
