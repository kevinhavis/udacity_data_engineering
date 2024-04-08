import datetime
from distutils.util import execute

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    "Copy data from source S3 data source to Redshift DB."
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        target_table="",
        s3_bucket="",
        s3_key="",
        json_path="",
        *args,
        **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        self.log.info("Connecting to AWS.")

        # Create AWS and Redshift connections
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Deleting existing tables
        self.log.info("Deleting existing table {}".format(self.target_table))
        redshift.run("DELETE FROM {}".format(self.target_table))

        self.log.info(
            "Copying table {} data to staging Redshift location.".format(
                self.target_table
            )
        )

        # Copy data
        rendered_key = self.s3_key.format(**context)
        self.log.info("Rendered key is {}.".format(rendered_key))
        s3_path = "{}".format(self.s3_bucket)
        self.log.info("S3 path is {}.".format(s3_path))
        self.log.info("Target table is {}.".format(self.target_table))
        self.log.info("aws_credentials is {}.".format(aws_credentials.access_key))
        self.log.info("aws_credentials secret is {}.".format(aws_credentials.secret_key))
        self.log.info("json path is {}".format(self.json_path))
        
        stage_sql = (
            """
            COPY {} 
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT as json '{}'
            REGION 'us-west-2'
            TIMEFORMAT 'epochmillisecs'
            """
        ).format(
            self.target_table,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.json_path
        )
        self.log.info("SQL is: {}".format(stage_sql))

        redshift.run(stage_sql)
        self.log.info("Completed copying of S3 data to Redshift.")
