import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def test_data(cur, conn):
    print("Checking: Table users content")
    cur.execute(users_head)
    results = cur.fetchone()
    print(results, "\n")
    print("Checking: Table users row count")
    cur.execute(users_count)
    results = cur.fetchone()
    print(results[0], "\n")
    print("Checking: Table songs content")
    cur.execute(songs_head)
    results = cur.fetchone()
    print(results, "\n")
    print("Checking: Table songs row count")
    cur.execute(songs_count)
    results = cur.fetchone()
    print(results[0], "\n")
    print("Checking: Table artists content")
    cur.execute(artists_head)
    results = cur.fetchone()
    print(results, "\n")
    print("Checking: Table artists row count")
    cur.execute(artists_count)
    results = cur.fetchone()
    print(results[0], "\n")
    print("Checking: Table time content")
    cur.execute(time_head)
    results = cur.fetchone()
    print(results, "\n")
    print("Checking: Table time row count")
    cur.execute(time_count)
    results = cur.fetchone()
    print(results[0], "\n")
    print("Checking: Table songplays content")
    cur.execute(songplays_head)
    results = cur.fetchone()
    print(results, "\n")
    print("Checking: Table songplays row count")
    cur.execute(songplays_count)
    results = cur.fetchone()
    print(results[0], "\n")
    print("Checking: Table songplays where song_select works")
    cur.execute(songplays_where)
    results = cur.fetchone()
    print(results, "\n")
        
def main():
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
        
    # Adding error logging
    try:
        test_data(cur, conn)
    except psycopg2.Error as e:
        print("Error: Cannot verify the data")
        print(e)
        
    conn.close()

    
if __name__ == "__main__":
    main()