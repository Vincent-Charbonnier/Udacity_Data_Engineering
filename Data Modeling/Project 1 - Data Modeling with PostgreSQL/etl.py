import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    for index, row in df.iterrows():
        # insert song record
        song_data = [row.song_id, row.title, row.artist_id, row.year, row.duration]
        # Adding error logging
        try:
            cur.execute(song_table_insert, song_data)
        except psycopg2.Error as e:
            print("Error: Insert row for table songs")
            print (e)
    
        # insert artist record
        artist_data = [row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude]
        # Adding error logging
        try:
            cur.execute(artist_table_insert, artist_data)
        except psycopg2.Error as e:
            print("Error: Insert row for table artists")
            print (e)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the time information in order to store it into the time table.
    Then it extracts the user information in order to store it into the users table.
    Finally it extracts joins what we extracted before in order to store it into the songplays table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df.ts, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(columns=column_labels) 
    
    for index, column_label in enumerate(column_labels):
        time_df[column_label] = time_data[index]

    for i, row in time_df.iterrows():
        # Adding error logging
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Insert row for table time")
            print (e)

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        # Adding error logging
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Insert row for table users")
            print (e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        # Adding error logging
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
            # Adding error logging
            try:
                cur.execute(songplay_table_insert, songplay_data)
            except psycopg2.Error as e:
                print("Error: Insert row for table songplays")
                print (e)
        except psycopg2.Error as e:
            print("Error: Query for Song ID and Artist ID")
            print (e)

            
def process_data(cur, conn, filepath, func):
    """
    This procedure processes a data (JSON) file whose filepath has been provided as an arugment.

    INPUTS: 
    * cur the cursor variable
    * conn the connection variable
    * filepath the file path to the data file
    * func the function to call for each found file
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

        
def main():
    """
    This is the main script execution.
    
    First, it creates a database connection.
    Then, it processes both Song and Log information.
    Finally, it closes the cursor and database connection.
    """
    
    # Adding error logging
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
    
    # Adding error logging
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

    
if __name__ == "__main__":
    main()