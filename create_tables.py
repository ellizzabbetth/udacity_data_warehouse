import logging
from sql_queries import create_connection, create_table_queries, drop_table_queries

# drop staging tables?
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        logging.info('Create table {}'.format(query))
        cur.execute(query)
        conn.commit()

def main():
    cur, conn = create_connection()
    print('Connected to Redshift Cluster...')
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()