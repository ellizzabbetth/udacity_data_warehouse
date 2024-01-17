





#### Setup & Configuration
1. Setup your IAM user with programmatic Access Key and Secret Key with the following commands:

setx AWS_ACCESS_KEY_ID ********
setx AWS_SECRET_ACCESS_KEY ******
setx AWS_DEFAULT_REGION us-west-2

 [How to set environment variables] (https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)

2. Create dwh.cfg file. Then populate with custom values using dwh_template as an example.

3. Notice that we use song_data = 's3://udacity-dend/song-data/A/A/A'. This is because using song_data = 's3://udacity-dend/song-data' 
took more than one hour to copy from S3 to Redshift.
 
## Summary



### Schema for Song Play Analysis
Using the song and event datasets in S3, a star schema was created optimized for queries on song play analysis.

### Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday





## Install

```bash
$ pip install -r requirements.txt
```

## Files

**`create_cluster.py`**

* Create IAM role, Redshift cluster, and allow TCP connection from outside VPC
* Pass `--delete` flag to delete resources

**`create_tables.py`**  Drop and recreate tables

**`dwh.cfg`**           Configure Redshift cluster and data import

**`etl.py`**            Copy data to staging tables and insert into star schema fact and dimension tables

**`sql_queries.py`**

* Creating and dropping staging and star schema tables
* Copy JSON data from S3 to Redshift staging tables
* Insert data from staging tables to star schema fact and dimension tables
* Holds validation and test queries

**`analytics.py`**
* Inspect data in tables and execute exploratory queries

## Run scripts

Set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

Choose `DB and DB_PASSWORD` in `dhw.cfg`.

Create IAM role, Redshift cluster, and configure TCP connectivity.

```bash
$ python create_cluster.py
```

`create_cluster.py` updates `dwh.cfg` with the following outputs
* `[DB][HOST]`
* `[IAM_ROLE][ARN]`

Drop and recreate tables

```bash
$ python create_tables.py
```

Run ETL pipeline

```bash
$ python etl.py
```

Run Analytics 

```bash
$ python analytics.py
```

Delete IAM role and Redshift cluster
```bash
$ python delete_cluster.py
OR
$ python create_cluster.py --delete
```

Create plots
```bash
    Run cells in Jupyter notebook
```

## Resulting Count

Running:
SELECT COUNT(*) FROM staging_events
8056

Running:
SELECT COUNT(*) FROM staging_songs
24

Running:
SELECT COUNT(*) FROM songplay
6820

Running:
SELECT COUNT(*) FROM users
104

Running:
SELECT COUNT(*) FROM songs
24

Running:
SELECT COUNT(*) FROM artists
24

Running:
SELECT COUNT(*) FROM time
6813

## Next steps

* Add additional data quality checks -- song table has too many records with song_id null
* Creation of a dashboard for analytic queries using Power BI

