import configparser
import psycopg2
import logging
from sql_queries import create_connection, copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        conn.commit()
        print('{} processed OK.'.format(query))
    print('All files COPIED to staging tables.')


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('\n'.join(('', 'Inserting into STAR SCHEMA:', query)))
        cur.execute(query)
        conn.commit()
        print('{} processed OK.'.format(query))
    print('All files INSERTED into staging tables.')

def main():
    print('Initiate ETL...')
    print('Connecting to Redshift Cluster...')
    cur, conn = create_connection()
    
    load_staging_tables(cur, conn)  
    insert_tables(cur, conn)

    print('Staging tables created and hydrated.')

    conn.close()


if __name__ == "__main__":
    main()