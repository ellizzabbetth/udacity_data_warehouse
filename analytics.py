
import pandas as pd
import logging
from sql_queries import create_connection, test_queries, validation_queries

def table_counts(cur, conn):
    """
    Return the number of rows stored into each table
    """
    for query in validation_queries:
        logging.info('Validate data {}'.format(query))
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)


def execute_test_queries(cur, conn):
    for query in test_queries:
        try:
            logging.info('Query data in final tables. {}'.format(query))
            print(query)
            cur.execute(query)
            rows = cur.fetchall()
            print(pd.DataFrame(rows))
            # for row in rows:
            #     print(row)

        except Exception as e:
            print(e)



def sample_data_from_tables(cur):
    """ Print a sample of the data in each of the final tables.

        Args:
        * cur: the cursor to the db connection
    """
    tables = (
                ("staging_events", 
                    ("artist", "auth", "firstName", "gender" ,
                     "itemInSession", "lastName", "length", "level", 
                     "location", "method", "page", "registration",
                     "sessionId" ,"song", "status" ,"ts", "userAgent", "userId")
                ),
                ("staging_songs", 
                    ("num_songs", "artist_id", "artist_latitude", "artist_longitude", "artist_location", 
                     "artist_name", "song_id", "title", "duration", "year")
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
                    ("songplay_id", "start_time", "user_id", "level", "song_id",
                    "artist_id", "session_id", "location", "user_agent")
                )
            )
        
    for table, cols in tables:
        query = """
            SELECT * 
              FROM {}
             ORDER BY random()
             LIMIT {};
        """.format(table, 20)

        logging.info('Sample of data in final tables. {}'.format(query))
        try:
            cur.execute(query)
            rows = cur.fetchall()
           # logging.info('Sample of data in {}: {}'.format(table, query))
            print(pd.DataFrame(rows, columns=cols))

        except Exception as e:
            print(e)

def main():
    """
    Runs analytical queries
    """
    cur, conn = create_connection()
    print('Connected to Redshift Cluster...')

    # View a sample of data for sanitation
    sample_data_from_tables(cur)
    # Analytical queries
    table_counts(cur, conn)
    execute_test_queries(cur, conn)
  
    logging.info('Exiting...')
    conn.close()


if __name__ == "__main__":
    main()