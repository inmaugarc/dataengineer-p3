import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Function to load staging tables """
    """ INPUT: cur:cursor, conn:connection """
    """ OUTPUT: data copied into staging tables"""

    for query in copy_table_queries:
        #print("Ejecutando: ",query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Function to insert data into star schema datawarehouse, with facts and dimension tables """
    """ INPUT: cur:cursor, conn:connection """
    """ OUTPUT: data distributed in the proper table"""

    for query in insert_table_queries:
        #print("Estamos en la ",query)
        cur.execute(query)
        conn.commit()


def main():
    """ Main function that orchestrates everything """
    """ This main function reads the configuration file and connect to the redshift cluster """
    """ And also load the data to the staging tables and finally insert data into the tables of the star schema """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("dbname={} user={} password={} port={} host={}".format(*config['DB'].values()))
    #print("Preparamos cadena de conexión:",conn)
    cur = conn.cursor()
    #print("Nos conectamos a la bd")
    
    load_staging_tables(cur, conn)
   # print("Cargamos las tablas staging")
    insert_tables(cur, conn)
    print("Datos insertados")

    conn.close()


if __name__ == "__main__":
    main()
