from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket='',
                 s3_prefix='',
                 table='',
                 redshift_conn_id='',
                 aws_credentials_id='',
                 copy_options='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.copy_options = copy_options

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info('Connecting to Redshift...')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Staging data to from s3 to Redshift Cluster')

        query = f"""
                COPY {self.table}
                FROM 's3://{self.s3_bucket}/{self.s3_prefix}'
                WITH credentials 'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
                {self.copy_options};
            """

        self.log.info('Executing COPY...')
        redshift_hook.run(query)
        self.log.info("Data successfully staged to redshift")



