import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """ Function to delete tables """
    """ It deletes all tables in drop_table_queries """
    """ INPUT cur: cursor, conn:connection to DB """
    """ OUTPUT None (Deleted tables) """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """ Function to create tables """    
    """ This function creates all tables in create_table_queries """
    """ INPUT cur: cursor, conn:connection to DB """
    """ OUTPUT None (Created tables) """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """ Main function that orchestrates everything """
    """ This main function reads the configuration file and create a redshift cluster with an open TCP connection """
    """ And after the Redshift cluster is created, then delete tables and then create a new ones"""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #conn_string="postgresql://{}:{}@{}:{}/{}".format(dwh_db_user, dwh_db_password, dwh_endpoint, dwh_port, dwh_db)
    #print(conn_string)
    
    conn = psycopg2.connect("dbname={} user={} password={} port={} host={}".format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()