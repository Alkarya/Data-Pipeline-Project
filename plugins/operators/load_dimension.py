from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='redshift',
                 sql = '',
                 load_mode = 'append',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.load_mode = load_mode

    def execute(self, context):
        self.log.info(f'Running LoadDimensionOperator for {self.table}')

        redshift_hook = PostgresHook("redshift")

        if self.load_mode == 'delete':
            self.log.info('Executing load in delete&load mode...')
            self.log.info(f'Deleting data from {self.table}')
            
            redshift_hook.run(f'DELETE FROM {self.table};')
        
        self.log.info(f'Inserting data in {self.table} table...')

        insert_query = f"""
            INSERT INTO {self.table}
            {self.sql}
        """

        redshift_hook.run(insert_query)

        self.log.info(f'Loading data for {self.table} table is now complete')
