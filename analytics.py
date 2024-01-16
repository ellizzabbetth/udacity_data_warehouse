
import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
import os
import glob
import logging

from sql_queries import create_connection,  test_queries, validation_queries

def get_tables_rows(cur, conn):
    """
    Gets the number of rows stored into each table
    """
    for query in validation_queries:
        logging.info('cluster created {}'.format(query))
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)

def make_queries(cur, conn):
    print("=== Runs tests...")
    for query in test_queries:
        try:
            print(query)
            cur.execute(query)
            rows = cur.fetchall()
            print(pd.DataFrame(rows))

        except Exception as e:
            print(e)



def check_tables(cur):
    """ Print a subsample of the data in each of the final tablesk.

        Args:
        * cur: the cursor to the db connection
    """
    tables = (
                ("staging_events", 
                    ("artist", "auth", "firstName", "gender" ,
                     "itemInSession","lastName","length", "level", 
                     "location", "method", "page", "registration",
                     "sessionId" ,"song", "status" ,"ts","userAgent", "userId")
                ),
                ("staging_songs", 
                    ("num_songs", "artist_id", "artist_latitude", "artist_longitude", "artist_location", 
                     "artist_name", "song_id",  "title", "duration", "year")
                ),
                ("users", ("user_id", "first_name", "last_name", "gender", "level")
                ),
                ("songs", ("song_id", "title", "artist_id", "year", "duration")
                ),
                ("artists", ("artist_id", "name", "location", "latitude", "longitude")
                ),
                ("time", ("start_time", "hour", "day", "week", "month", "year", "weekday")
                ),
                ("songplay",
                    ("songplay_id","start_time","user_id", "level", "song_id",
                    "artist_id", "session_id", "location", "user_agent" )
                )
            )
        
    for table, cols in tables:
        query = """
            SELECT * 
              FROM {}
             ORDER BY random()
             LIMIT {};
        """.format(table, 10)

        print(f"\n===== {table} ===== ")

        try:
            cur.execute(query)

            rows = cur.fetchall()
            print(pd.DataFrame(rows, columns=cols))

        except Exception as e:
            print(e)

def main():
    """
    Runs analytical queries
    """
    cur, conn = create_connection()
    print('Connected to Redshift Cluster...')


    # print a sample of data for sanitation
    check_tables(cur)
    
    # Analytical queries
    get_tables_rows(cur, conn)

    make_queries(cur, conn)

   
    logging.info('Exiting...')
    
    conn.close()


if __name__ == "__main__":
    main()