from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id = redshift_conn_id
        self.query=query

    def execute(self, context):
        self.log.info('Grabbing Postgres Hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Emptying Table')
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info(f'Running query {self.query}')
        redshift.run(self.query)