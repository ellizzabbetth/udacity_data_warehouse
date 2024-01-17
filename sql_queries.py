import configparser
from create_cluster import config_file
import psycopg2
import logging


# CONFIG
config = configparser.ConfigParser()
config.read(config_file)

ARN             = config['IAM_ROLE']['ARN']
LOG_DATA        = config['S3']['LOG_DATA']
LOG_JSONPATH    = config['S3']['LOG_JSONPATH']
SONG_DATA       = config['S3']['SONG_DATA']
SONGS_JSONPATH  = config['S3']['SONGS_JSONPATH']


def create_connection():
    """Creates Redshift Connection

    Returns:
        SQL Connection Object: Cursor and Connection objects used to execute queries
    """
    # Variables to create connection to Redshift Cluster
    host =          config.get('DB', 'HOST')
    db_name =       config.get('DB', 'DB_NAME')
    db_username =   config.get('DB', 'DB_USER')
    db_password =   config.get('DB', 'DB_PASSWORD')
    port =          config.getint('DB', 'DB_PORT')

    # Connecting to Redshift Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, db_name, db_username, db_password, port))
    cur = conn.cursor()
    return cur, conn


# STAGING TABLES
# timeformat as 'epochmillisecs' 
staging_events_copy = ("""
COPY staging_events
FROM {0}
iam_role '{1}'
region 'us-west-2'
json {2}
                      
BLANKSASNULL
EMPTYASNULL
TRUNCATECOLUMNS
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role '{}'
    region 'us-west-2'
    JSON 'auto' MAXERROR 50
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop =       "DROP TABLE IF EXISTS songplay;"
user_table_drop =           "DROP TABLE IF EXISTS users;"
song_table_drop =           "DROP TABLE IF EXISTS songs;"
artist_table_drop =         "DROP TABLE IF EXISTS artists;"
time_table_drop =           "DROP TABLE  IF EXISTS time;"




# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist          VARCHAR(1024),
        auth            VARCHAR(1024),
        firstName       VARCHAR(1024),
        gender          VARCHAR(1024),
        itemInSession   INTEGER,
        lastName        VARCHAR(1024),
        length          REAL,
        level           VARCHAR(1024),
        location        VARCHAR(1024),
        method          VARCHAR(1024),
        page            VARCHAR(1024) sortkey,
        registration    DOUBLE PRECISION,
        sessionId       INTEGER,
        song            VARCHAR(1024) distkey,
        status          INTEGER,
        ts              BIGINT,
        userAgent       VARCHAR(65535),
        userId          VARCHAR(1024)
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR(1024),
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     VARCHAR(1024),
        artist_name         VARCHAR(1024),
        song_id             VARCHAR(1024),
        title               VARCHAR(1024) distkey,
        duration            REAL,
        year                VARCHAR(1024)
    );
""")


songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id     BIGINT IDENTITY(0, 1) NOT NULL PRIMARY KEY, 
        start_time      BIGINT, 
        user_id         TEXT DISTKEY, 
        level           TEXT,
        song_id         TEXT, 
        artist_id       TEXT, 
        session_id      INTEGER, 
        location        TEXT sortkey, 
        user_agent      TEXT
    );
""")


user_table_create = ("""
    CREATE TABLE users (
        user_id         TEXT NOT NULL PRIMARY KEY DISTKEY, 
        first_name      TEXT, 
        last_name       TEXT,
        gender          TEXT,
        level           TEXT
    );

""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id         TEXT NOT NULL PRIMARY KEY  DISTKEY, 
        title           TEXT, 
        artist_id       TEXT, 
        year            INTEGER,
        duration        FLOAT
    );
    """
)

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id       TEXT NOT NULL PRIMARY KEY, 
        name            TEXT, 
        location        TEXT, 
        latitude        FLOAT, 
        longitude       FLOAT
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time      BIGINT PRIMARY KEY, 
        hour            INTEGER, 
        day             INTEGER, 
        week            INTEGER, 
        month           INTEGER, 
        year            INTEGER, 
        weekday         INTEGER
    )
    diststyle all;
""")



# FINAL TABLES
#  JOIN condition should be on song title, artist name and song duration.

songplay_table_insert = ("""
   INSERT INTO songplay (start_time, user_id, level, song_id, 
            artist_id, session_id, location, user_agent)
    SELECT ts AS start_time, 
           e.userId AS user_id, 
           e.level AS level, 
           s.song_id AS song_id, 
           s.artist_id AS artist_id, 
           e.sessionId AS session_id, 
           e.location AS location, 
           e.userAgent AS user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s 
    ON e.song = s.title
    AND
        e.artist = s.artist_name 
   
    WHERE
      e.page = 'NextSong'

""")

#  AND
        # ABS(e.length - s.duration) < 12


user_table_insert = ("""
    INSERT INTO users
    WITH numbered_levels AS (
      SELECT ROW_NUMBER() over (PARTITION by userId ORDER BY ts DESC) AS row_num,
             userId AS user_id,
             firstName AS first_name, 
             lastName AS last_name, 
             gender, 
             level
        FROM staging_events
    )
    SELECT DISTINCT user_id, first_name, last_name, gender, level
      FROM numbered_levels
     WHERE row_num = 1 and user_id is not null
    
""")

# user_table_insert = ("""
#      INSERT INTO users (user_id,
#      first_name,
#      last_name,
#      gender,
#      level)
#     SELECT  DISTINCT(userId)    AS user_id,
#             firstName           AS first_name,
#             lastName            AS last_name,
#             gender,
#             level
#     FROM staging_events
#     WHERE user_id IS NOT NULL
#     AND page  =  'NextSong';
# """)

song_table_insert = ("""
    INSERT INTO songs 
    SELECT distinct song_id,
           title,
           artist_id,
           CAST(year AS INTEGER),
           duration
      FROM staging_songs
      WHERE song_id IS NOT NULL
                     
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT distinct artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
      FROM staging_songs
      WHERE artist_id IS NOT NULL
      ORDER BY artist_id;

""")

time_table_insert = ("""
    INSERT INTO time 
    SELECT DISTINCT ts AS start_time,
           EXTRACT(hour     FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS hour,
           EXTRACT(day      FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS day,
           EXTRACT(week     FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS week,
           EXTRACT(month    FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS month,
           EXTRACT(year     FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year,
           EXTRACT(weekday  FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS weekday
      FROM staging_events
     WHERE page = 'NextSong'
""")

import configparser

# List of data types for amazon redshift
# https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_event"
staging_songs_table_drop =  "DROP TABLE IF EXISTS stage_song"
songplay_table_drop =       "DROP TABLE IF EXISTS songplays"
user_table_drop =           "DROP TABLE IF EXISTS users"
song_table_drop =           "DROP TABLE IF EXISTS songs"
artist_table_drop =         "DROP TABLE IF EXISTS artists"
time_table_drop =           "DROP TABLE IF EXISTS time"




# ANALYTICAL QUERIES
count_staging_events = "SELECT COUNT(*) FROM staging_events"
count_staging_songs =  "SELECT COUNT(*) FROM staging_songs"
count_songplay =       "SELECT COUNT(*) FROM songplay"
count_users =          "SELECT COUNT(*) FROM users"
count_songs =          "SELECT COUNT(*) FROM songs"
count_artists =        "SELECT COUNT(*) FROM artists"
count_time =           "SELECT COUNT(*) FROM time"



# TEST QUERIES
# Top 15 super users
test1 = (
"""
WITH super_users AS (
    SELECT  user_id, COUNT(*) AS cnt
    FROM songplay
    GROUP BY user_id
    ORDER BY cnt DESC
    LIMIT 15
)
SELECT users.first_name, 
       users.last_name, 
       super_users.cnt
  FROM super_users
 INNER JOIN users
       ON users.user_id = super_users.user_id
       
 ORDER BY cnt DESC
"""
)
# 50 most popular locations where songs are played
test2 = (
"""
SELECT location, 
       count(*) AS cnt 
  FROM songplay
 GROUP BY location 
 ORDER BY cnt DESC 
 LIMIT 50
"""
)

test3 = (
    """
  SELECT sp.song_id, s.title, count(*) AS cnt 
    FROM songplay sp
    JOIN songs s
      ON sp.song_id = s.song_id
GROUP BY 1, 2
ORDER BY 3 DESC
   LIMIT 10;
    """
)


test4 = (
# print('Statistics on when songs are played during a day')

    """
  SELECT CASE
           WHEN t.hour BETWEEN 2 AND 7  THEN '2~7'
           WHEN t.hour BETWEEN 8 AND 12 THEN '8~12'
           WHEN t.hour BETWEEN 13 AND 18 THEN '13~18'
           WHEN t.hour BETWEEN 19 AND 22 THEN '19~22'
           ELSE '23~24, 0~2'
         END AS play_time, 
         count(*) AS cnt
    FROM songplay sp
    JOIN time t
      ON sp.start_time = t.start_time
GROUP BY 1
ORDER BY 2 DESC;
    """
)

test5 = (
    """
SELECT distinct sp.songplay_id,
        u.user_id,
        u.last_name,
        u.first_name,
        sp.start_time,
        sp.song_id
      
FROM songplay AS sp
        JOIN users   AS u ON (u.user_id = sp.user_id)

where sp.song_id is not NULL
ORDER BY (u.last_name)
LIMIT 10;
    """
)

test6 = (
    """
    select
    song_id
    from songs
    """
)

test7 = (
    """
    select distinct
    *
    from users order by last_name
    """
)

# most played songs by year
test8 = (
"""
WITH max_song AS (
    SELECT  year, COUNT(*) AS cnt
    FROM songs
    GROUP BY year
    ORDER BY cnt DESC
    LIMIT 15
)
SELECT title, songs.year,
       max_song.cnt 
  FROM max_song inner join songs
  on songs.year = max_song.year

 ORDER BY cnt DESC, year asc
 LIMIT 30
"""
)


# """
# WITH super_users AS (
#     SELECT  user_id, COUNT(*) AS cnt
#     FROM songplay
#     GROUP BY user_id
#     ORDER BY cnt DESC
#     LIMIT 15
# )
# SELECT users.first_name, 
#        users.last_name, 
#        super_users.cnt
#   FROM super_users
#  INNER JOIN users
#        ON users.user_id = super_users.user_id
       
#  ORDER BY cnt DESC
# """

test9 = (
    """
    select * from songs s
    join artists a on s.artist_id = a.artist_id

    """
)
# """
# WITH super_users AS (
#     SELECT  user_id, COUNT(*) AS cnt
#     FROM songplay
#     GROUP BY user_id
#     ORDER BY cnt DESC
#     LIMIT 15
# )
# SELECT users.first_name, 
#        users.last_name, 
#        super_users.cnt
#   FROM super_users
#  INNER JOIN users
#        ON users.user_id = super_users.user_id
       
#  ORDER BY cnt DESC
# """

# QUERY LISTS
create_table_queries =  [
    staging_events_table_create, 
    staging_songs_table_create, 
    user_table_create, 
    song_table_create, 
    artist_table_create, 
    time_table_create, 
    songplay_table_create
    ]

drop_table_queries =    [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop
]

copy_table_queries =    [
    staging_events_copy, 
    staging_songs_copy
]

insert_table_queries =  [
    songplay_table_insert, 
    user_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert
]

validation_queries = [
    count_staging_events,
    count_staging_songs,
    count_songplay,
    count_users,
    count_songs,
    count_artists,
    count_time,
]

test_queries = [
    test1,
    test2,
    test3,
    test4,
    test5,
    test6,
    test7,
    test8,
    test9
]