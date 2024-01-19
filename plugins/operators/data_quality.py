from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[]
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Running Data Quality Process')
        self.log.info('Connecting to Redshift...')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        num_quality_checks = len(self.tables)
        current_check = 1
        self.log.info(f"Starting data quality checks...")

        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. The {table} returned no results")          
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. The {table} contained 0 rows")
                
            self.log.info("Check Passed")
            current_check = current_check + 1
                
        self.log.info(f'All {num_quality_checks} checks passed.')