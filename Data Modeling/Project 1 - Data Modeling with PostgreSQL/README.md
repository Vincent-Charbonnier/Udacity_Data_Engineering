# Project 1 - Data Modeling with PostgreSQL

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.
  
## Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", 
"artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

## Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.
The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![image](https://user-images.githubusercontent.com/70199241/172784478-a2c568d5-640e-48a2-911e-d0fc41c2b517.png)

If you would like to look at the JSON data within log_data files, you will need to create a pandas dataframe to read the data. Remember to first import JSON and pandas libraries.
```
df = pd.read_json(filepath, lines=True)
```
For example, ```df = pd.read_json('data/log_data/2018/11/2018-11-01-events.json', lines=True)``` would read the data file ```2018-11-01-events.json```.

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

![image](https://user-images.githubusercontent.com/70199241/179716380-9399fc04-eb13-40bf-86bb-3d457fffdb76.png)

https://drawsql.app/hpe-1/diagrams/song-play-analysis

### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**[WARNING] The songplays table does not have a primary key!**  

### Dimension Tables
2. users - users in the app
  - user_id, first_name, last_name, gender, level    
3. songs - songs in music database
  - song_id, title, artist_id, year, duration    
4. artists - artists in music database
  - artist_id, name, location, latitude, longitude  
5. time - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

## Project Template
In addition to the data files, the project repository includes 7 files:
1. create_tables.py: drops and creates the tables. You must run this file to reset the tables before each time you run the ETL scripts.
2. etl.ipynb: reads and processes a single file from song_data and log_data and loads the data into the tables. This notebook contains detailed instructions on the ETL process for each of the tables.
3. etl.py: reads and processes files from song_data and log_data and loads them into your tables. This file has been created based on the ETL notebook.
4. test.ipynb: displays the first few rows of each table to let you check the database.
5. test.py: displays the first few rows of each table to let you check the database.
6. sql_queries.py: contains all the sql queries, and is imported into the last three files above.
7. README.md: provides the project's description.

## Try the project
To try the project, open a terminal and run the following:

1. ``` python create_tables.py``` *to create your database and tables.*

2. ``` python etl.py``` *to develop ETL processes for each table*

3. ``` python test.py``` *to verify if the database is correctly set*

**[WARNING] Remember to run create_tables.py before running etl.py to reset your tables.**

