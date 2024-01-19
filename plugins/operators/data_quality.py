from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 operations=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.operations = operations

    def execute(self, context):
        self.log.info('Running Data Quality Process')
        redshift_hook = PostgresHook("redshift")

        num_quality_checks = len(self.operations)
        current_check = 1
        self.log.info(f"Starting data quality checks...")

        for op in self.operations:

            self.log.info(f"Check {current_check}/{num_quality_checks}")

            result = int(redshift_hook.get_first(sql=op['sql'])[0])

            if op['check'] == 'greater':
                if result <= op['value']:
                    raise ValueError(f'Data Quality check failed {result} <= {op["value"]}')
                
            if op['check'] == 'equal':
                if result != op['value']:
                    raise ValueError(f'Data Quality check failed {result} != {op["value"]}')
                
            self.log.info("Check Passed")
            current_check = current_check + 1
                
        self.log.info(f'All {num_quality_checks} passed.')