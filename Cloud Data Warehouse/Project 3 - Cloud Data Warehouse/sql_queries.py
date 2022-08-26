import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Column names written based on log_data files output
staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionID INTEGER,
    song VARCHAR,
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INTEGER
);
""")

# Column names written based on song_data files output
staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL,
    year INTEGER 
);
""")

# songplays table is a small table, no need for distkey
songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INTEGER IDENTITY(0,1) NULL sortkey,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR)
diststyle all;
""")

# user table is a small table, no need for distkey
user_table_create = ("""
CREATE TABLE users(
    user_id INTEGER  PRIMARY KEY sortkey,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR 
);
""")

# song table has over 10 thousands of records artist_id used as distkey
song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR  PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR distkey,
    year INTEGER,
    duration DECIMAL)
diststyle all;
""")

# artist table has over 14 thousands of records artist_id used as distkey
artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR  PRIMARY KEY distkey,
    name VARCHAR,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL 
);
""")

# time table has over 8 thousands of records artist_id used as distkey
time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP  PRIMARY KEY distkey,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER 
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON {};
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

# Get distinct (no duplicate) data from joined staging_events & staging_songs tables and insert it into songplays table
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TO_DATE(se.ts,'dd.mm.yyyy/hh:mi:ss') AS start_time,
                se.userId                            AS user_id,
                se.level                             AS level,
                ss.song_id                           AS song_id,
                ss.artist_id                         AS artist_id,
                se.sessionID                         AS session_id,
                se.location                          AS location,
                se.userAgent                         AS user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.song = ss.title) AND (se.artist = ss.artist_name)
WHERE se.ts IS NOT NULL;
""")

# Get distinct (no duplicate) data from staging_events table and insert it into users table
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.userId    AS user_id,
                se.firstName AS first_name,
                se.lastName  AS last_name,
                se.gender    AS gender,
                se.level     AS level
FROM staging_events se
WHERE se.userId IS NOT NULL;
""")

# Get distinct (no duplicate) data from staging_songs table and insert it into songs table
song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT  ss.song_id      AS song_id,
        ss.title        AS title,
        ss.artist_id    AS artist_id,
        ss.year         AS year,
        ss.duration     AS duration
FROM staging_songs ss
WHERE ss.song_id IS NOT NULL;
""")

# Get distinct (no duplicate) data from staging_songs table and insert it into artists table
artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT ss.artist_id        AS artist_id,
                ss.artist_name      AS name,
                ss.artist_location  AS location,
                ss.artist_latitude  AS latitude,
                ss.artist_longitude AS longitude
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL;
""")

# Get distinct (no duplicate) data from staging_events table and insert it into time table
time_table_insert = ("""
INSERT INTO time(start_time, hour, day, month, week, year, weekday)
SELECT DISTINCT TO_DATE(se.ts,'dd.mm.yyyy/hh:mi:ss') AS start_time,
                DATEPART(HOUR,start_time)            AS hour,
                DATEPART(DAY,start_time)             AS day,
                DATEPART(MONTH,start_time)           AS month,
                DATEPART(WEEK,start_time)            AS week,
                DATEPART(YEAR,start_time)            AS year,
                DATEPART(WEEKDAY,start_time)         AS weekday
FROM staging_events se
WHERE se.ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
