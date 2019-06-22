import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ It executes all copy queries in sql_queries.py, 
        which copy data from S3 to Redshift.
    
        Parameters: 
           cur: Database cursor
           conn: Database connection object
    
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ It executes all insert statements in sql_queries.py, 
        which insert data to fact and dimension tables.
    
        Parameters: 
           cur: Database cursor
           conn: Database connection object
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """ 
        It connects to Amazon Redshift and calls load_staging_tables 
        and insert_tables functions.
        
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()