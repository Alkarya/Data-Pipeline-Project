from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_file='',
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_file = sql_file

    def execute(self, context):
        # Creates tables if they do not exist
        self.log.info('Connecting to Redshift...')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Creating tables')
        with open(self.sql_file, 'r') as f:
            sql_queries = f.read()
            redshift_hook.run(sql_queries)

        self.log.info('Tables created sucessfully')



