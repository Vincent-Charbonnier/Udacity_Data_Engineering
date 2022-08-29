import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import boto3
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, FloatType as Flt, TimestampType
import pyspark.sql.functions as F
import pandas as pd

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

Bucket = "project4-data-lake-s3"
BucketPath = "s3a://" + Bucket
Parquet = BucketPath + "/parquet/"

def create_spark_session():
    """
        Creates the spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Gets the song data from the s3 bucket and creates the 
        artists and songs tables
        
        Parameters:
        spark: spark session
        input_data: input data path
        output_data: output data path
    """
    
    # get filepath to song data file
    SongBucketPath = BucketPath + "/song-data"
    #song_data = SongBucketPath + "/*/*/*/*.json"
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # define required schema
    SongDataSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Flt()),
        Fld("artist_longitude", Flt()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Flt()),
        Fld("year", Int())
    ])
    
    # read song data file
    dfSongData = spark.read.json(song_data, schema = SongDataSchema)

    # extract columns to create songs table
    songs_table = dfSongData.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("Overwrite").parquet(Parquet + "/songs")

    # extract columns to create artists table
    artists_table = dfSongData.select("artist_id", \
                                      col("artist_name").alias("name"), \
                                      col("artist_location").alias("location"), \
                                      col("artist_latitude").alias("latitude"), \
                                      col("artist_longitude").alias("longitude"))
    
    # write artists table to parquet files
    artists_table.write.mode("Overwrite").parquet(Parquet + "/artists")


def process_log_data(spark, input_data, output_data):
    """
        Gets the log data from the s3 bucket and creates the 
        users, time and songplays tables
        
        Parameters:
        spark: spark session
        input_data: input data path
        output_data: output data path
    """
    
    # get filepath to log data file
    LogBucketPath = BucketPath + "/log-data"
    #log_data = LogBucketPath + "/*.json"
    log_data = input_data + 'log_data/*.json'
    
    # define required schema
    LogDataSchema = R([
        Fld("artist",Str()),
        Fld("auth",Str()),
        Fld("firstName",Str()),
        Fld("gender",Str()),
        Fld("itemInSession",Int()),
        Fld("lastName",Str()),
        Fld("length", Flt()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Flt()),
        Fld("sessionId", Int()),
        Fld("song", Str()),
        Fld("status", Int()),
        Fld("ts", Str()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])
    
    # read log data file
    dfLogData = spark.read.json(log_data, schema = LogDataSchema)
    
    # filter by actions for song plays
    dfLogData = dfLogData.select('*').where(dfLogData.page == 'NextSong')

    # extract columns for users table    
    users_table = dfLogData.select(col("userId").alias("user_id"), \
                                   col("firstName").alias("first_name"), \
                                   col("lastName").alias("last_name"), \
                                   "gender", \
                                   "level")
    
    # write users table to parquet files
    users_table.write.mode("Overwrite").parquet(Parquet + "/users")
    
    # convert ts to double
    dfLogData = dfLogData.withColumn("ts",dfLogData.ts.cast(Dbl()))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: pd.Timestamp(x*1000000), TimestampType())
    dfLogData = dfLogData.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: pd.Timestamp(x*1000000), Date())
    dfLogData = dfLogData.withColumn("start_date", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = dfLogData.select("start_time")\
                        .withColumn("hour", F.hour(col("start_time")))\
                        .withColumn("day", F.dayofmonth(col("start_time")))\
                        .withColumn("week", F.weekofyear(col("start_time")))\
                        .withColumn("month", F.month(col("start_time")))\
                        .withColumn("year", F.year(col("start_time")))\
                        .withColumn("weekday", F.dayofweek(col("start_time")))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode("Overwrite").parquet(Parquet + "/time")

    # read in song data to use for songplays table // reset df
    dfSongData = spark.read.json(song_data, schema = SongDataSchema)
    
    # format and join
    dfSongData = dfSongData.selectExpr('title as song_title', '*')
    dfLogData = dfLogData.selectExpr('artist as artist_name', 'song as song_title','*')
    All = dfLogData.join(dfSongData, on=['song_title', 'artist_name'], how = 'outer')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = All.select("start_time", col("userId").alias("user_id"), "level", "song_id", "artist_id", col("SessionId").alias("session_id"), "artist_location", col("userAgent").alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode("Overwrite").parquet(Parquet + "/songplays")


def main():
    """
        1. Create the spark session
        2. Define the basic data paths (write and read)
        3. Create the star schema with both functions and 
           write them to the s3 bucket
    """
    
    spark = create_spark_session()
    
    #input_data = BucketPath
    #output_data = Parquet
    input_data = "s3a://project4-data-lake-s3/"
    output_data = "s3a://project4-data-lake-s3/parquet/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
