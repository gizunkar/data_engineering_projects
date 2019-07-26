from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException


class StageToRedshiftOperator(BaseOperator):
    """
    This operator copies JSON or CSV files from S3 to Redshift.

    :param task_id: a unique, meaningful id for the task
    :type task_id: string
    :param dag: a reference to the dag the task is attached to (if any)
    :type dag: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param aws_credentials_id: reference to a specific aws IAM user and password
    :type aws_credentials_id: string
    :param table: reference to target table name in redshift
    :type table: string
    :param s3_bucket: reference to s3 bucket
    :type s3_bucket: string
    :param s3_key: reference to s3 bucket path
    :type s3_key: string
    :param delimiter: reference to delimiter for csv file.
            If the copied files are JSON, this parameter should be ignored.
    :type delimiter: string
    :param ignore_headers: reference to headers of csv file if header is ignored.
            If the copied files are JSON, this parameter should be ignored.
    :type ignore_headers: number
    :param jsonpath: reference to jsonpath of JSON file.
            If the copied files are CSVs, this parameter should be ignored.
    :type jsonpath: string
    :param is_json: If the copied files are JSONS, this parameter should be True.
            If the copied files are CSVs, this parameter should be ignored.
    :type is_json: string
    :param is_csv: If the copied files are CSVs, this parameter should be True.
            If the copied files are JSONs, this parameter should be ignored.
    :type is_csv: string

"""

    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
        TIMEFORMAT as 'epochmillisecs'
        """
    copy_sql_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 jsonpath='auto',
                 is_json='',
                 is_csv='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.jsonpath = jsonpath
        self.is_json = is_json
        self.is_csv = is_csv

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.is_json == True:
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.jsonpath
            )
            redshift.run(formatted_sql)
        elif self.is_csv == True:
            formatted_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(formatted_sql)
        else:
            raise AirflowException("set is_json or is_csv True depending on the format of files.")
