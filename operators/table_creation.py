from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 create_sql='',
                 table ='',
                 *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql = create_sql
        self.table = table

    def execute(self, context):
        self.log.info('Getting Postgres hook')
        postgres_hook = PostgresHook(self.redshift_conn_id)
        
        try:
            self.log.info('Creating Tables')
            postgres_hook.run(self.create_sql)
            self.log.info('Tables generated')
        except:
            self.log.info('Tables already exist')


