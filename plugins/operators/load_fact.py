from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        self.log.info(f'Running LoadFactOperator for {self.table}')
        self.log.info('Connecting to Redshift...')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Inserting data in {self.table} table...')
        
        insert_query = f"""
            INSERT INTO {self.table}
            {self.sql}
        """

        redshift_hook.run(insert_query)

        self.log.info(f'Loading data for {self.table} table is now complete')

