# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE songplays ( \
    "songplay_id" INTEGER NOT NULL, \
    "start_time" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL, \
    "user_id" INTEGER NOT NULL, \
    "level" VARCHAR(255) NOT NULL, \
    "song_id" VARCHAR(255) NOT NULL, \
    "artist_id" VARCHAR(255) NOT NULL, \
    "session_id" INTEGER NOT NULL, \
    "location" VARCHAR(255) NOT NULL, \
    "user_agent" VARCHAR(255) NOT NULL \
); \
""")

user_table_create = ("CREATE TABLE users ( \
    "user_id" INTEGER NOT NULL, \
    "first_name" VARCHAR(255) NOT NULL, \
    "last_name" VARCHAR(255) NOT NULL, \
    "gender" CHAR(255) NOT NULL, \
    "level" VARCHAR(255) NOT NULL \
); \
")

song_table_create = ("CREATE TABLE songs ( \
    "song_id" VARCHAR(255) NOT NULL, \
    "title" VARCHAR(255) NOT NULL, \
    "artist_id" VARCHAR(255) NOT NULL, \
    "year" INTEGER NOT NULL, \
    "duration" DECIMAL(8, 2) NOT NULL \
); \
")

artist_table_create = ("CREATE TABLE artists ( \
    "artist_id" VARCHAR(255) NOT NULL, \
    "name" VARCHAR(255) NOT NULL, \
    "location" VARCHAR(255) NOT NULL, \
    "latitude" DECIMAL(8, 2) NOT NULL, \
    "longitude" DECIMAL(8, 2) NOT NULL \
); \
")

time_table_create = ("CREATE TABLE time ( \
    "start_time" INTEGER NOT NULL, \
    "hour" INTEGER NOT NULL, \
    "day" INTEGER NOT NULL, \
    "month" INTEGER NOT NULL, \
    "year" INTEGER NOT NULL, \
    "weekday" INTEGER NOT NULL \ 
); \
")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]