from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator raise Error if given table does not exist or has no records.

    :param task_id: a unique, meaningful id for the task
    :type task_id: string
    :param dag: a reference to the dag the task is attached to (if any)
    :type dag: string
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param table: reference to target table name in redshift
    :type table: string
    :param sql: reference to sql script that its result are inserted in redshift
    :type sql: string

    """


    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table= table
        self.sql=sql

    def execute(self, context):

        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))


        self.log.info("Inserting data from staging to dimension")
        formatted_sql = "INSERT INTO {} {}".format(
            self.table,
            self.sql
        )
        redshift.run(formatted_sql)
        self.log.info("Inserting into {} is done.".format(self.table))
