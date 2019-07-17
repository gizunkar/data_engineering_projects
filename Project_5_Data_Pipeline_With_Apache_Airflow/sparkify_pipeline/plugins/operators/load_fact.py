from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime

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

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        exec_date= context['execution_date']
        exec_day=exec_date.day
        exec_month= exec_date.month
        if exec_day == 15 :
            prev_date= datetime(exec_date.year, exec_month, 1)
        else:
            prev_date= datetime(exec_date.year, exec_month-1, 15)
        self.log.info("Inserting data to fact")
        formatted_sql = """INSERT INTO {} {} 
                            WHERE to_date ('{}', 'YYYY-MM-DD')< to_date(events.start_time, 'YYYY-MM-DD')  
                            and to_date(events.start_time, 'YYYY-MM-DD')  <=  to_date ('{}', 'YYYY-MM-DD')
                         """.format(
            self.table,
            self.sql,
            prev_date.date().isoformat(),
            exec_date.date().isoformat()
        )
        redshift.run(formatted_sql)
        
        self.log.info("Inserting into {} is done.".format(self.table))
            


