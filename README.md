





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

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

### Project Datasets
You'll be working with two datasets stored in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
To properly read log data s3://udacity-dend/log_data, you'll need the following metadata file:

Log metadata: s3://udacity-dend/log_json_path.json
Keep in mind that the udacity-dend bucket is situated in the us-west-2 region. If you're copying the dataset to Redshift located in us-east-1, remember to specify the region using the REGION keyword in the COPY command.

### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.


{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are file paths to two files in this dataset.


And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

log_data image
log_data image


![Alt text](./images/image.png)

### Schema for Song Play Analysis
Using the song and event datasets, a star schema was created optimized for queries on song play analysis. This includes the following tables.

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

**`analytics.py`**

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

## Further work

* Add additional data quality checks -- song table has too many records with song_id null
* Creation of a dashboard for analytic queries using Power BI

