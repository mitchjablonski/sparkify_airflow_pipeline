from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_qual_query="",
                 table="",
                 less_one_check=True,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.data_qual_query = data_qual_query
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.less_one_check = less_one_check

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if self.less_one_check:
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        else:
            if num_records > 0:
                raise ValueError(f"Data quality check failed. {self.table} contained {num_records} rows")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
