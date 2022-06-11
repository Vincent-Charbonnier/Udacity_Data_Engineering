# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("\
    CREATE TABLE songplays\
    ( \
        songplay_id serial PRIMARY KEY, \
        start_time bigint NOT NULL, \
        user_id int NOT NULL, \
        level varchar NOT NULL, \
        song_id varchar, \
        artist_id varchar, \
        session_id int NOT NULL, \
        location varchar NOT NULL, \
        user_agent varchar NOT NULL \
    )\
")

user_table_create = ("\
    CREATE TABLE users \
    ( \
        user_id int PRIMARY KEY, \
        first_name varchar NOT NULL, \
        last_name varchar NOT NULL, \
        gender char(1) NOT NULL, \
        level varchar NOT NULL \
    )\
")

song_table_create = ("\
    CREATE TABLE songs \
    ( \
        song_id varchar PRIMARY KEY, \
        title varchar NOT NULL, \
        artist_id varchar NOT NULL, \
        year int NOT NULL, \
        duration decimal NOT NULL \
    )\
")

artist_table_create = ("\
    CREATE TABLE artists \
    ( \
        artist_id varchar PRIMARY KEY, \
        name varchar NOT NULL, \
        location varchar NOT NULL, \
        latitude decimal NOT NULL, \
        longitude decimal NOT NULL \
    )\
")

time_table_create = ("\
    CREATE TABLE time \
    ( \
        start_time bigint PRIMARY KEY, \
        hour int NOT NULL, \
        day int NOT NULL, \
        week int NOT NULL, \
        month int NOT NULL, \
        year int NOT NULL, \
        weekday int NOT NULL \
    )\
")

# INSERT RECORDS

songplay_table_insert = ("\
    INSERT INTO songplays\
    (\
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent\
    )\
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)\
    ON CONFLICT DO NOTHING\
")

user_table_insert = ("\
    INSERT INTO users\
    (\
        user_id, first_name, last_name, gender, level\
    )\
    VALUES (%s, %s, %s, %s, %s)\
    ON CONFLICT (user_id) DO UPDATE SET level = excluded.level\
")

song_table_insert = ("\
    INSERT INTO songs\
    (\
        song_id, title, artist_id, year, duration\
    )\
    VALUES (%s, %s, %s, %s, %s)\
    ON CONFLICT DO NOTHING\
")

artist_table_insert = ("\
    INSERT INTO artists\
    (\
        artist_id, name, location, latitude, longitude\
    )\
    VALUES (%s, %s, %s, %s, %s)\
    ON CONFLICT DO NOTHING\
")

time_table_insert = ("\
    INSERT INTO time\
    (\
        start_time, hour, day, week, month, year, weekday\
    )\
    VALUES (%s, %s, %s, %s, %s, %s, %s)\
    ON CONFLICT DO NOTHING\
")

# FIND SONGS

song_select = ("\
    SELECT songs.song_id, artists.artist_id FROM songs \
    JOIN artists ON songs.artist_id = artists.artist_id \
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s \
    ")  

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# Test results

songplays_where = ("SELECT * FROM songplays WHERE song_id IS NOT NULL")
songplays_head = ("SELECT * FROM songplays LIMIT 1;")
songplays_count = ("SELECT count(*) FROM songplays*")
users_head = ("SELECT * FROM users LIMIT 1;")
users_count = ("SELECT count(*) FROM users")
songs_head = ("SELECT * FROM songs LIMIT 1;")
songs_count = ("SELECT count(*) FROM songs")
artists_head = ("SELECT * FROM artists LIMIT 1;")
artists_count = ("SELECT count(*) FROM artists")
time_head = ("SELECT * FROM time LIMIT 1;")
time_count = ("SELECT count(*) FROM time")