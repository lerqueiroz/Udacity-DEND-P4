import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a Spark framework session object

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ETL - Read Json files (song data), transform it and then save it on disk\
    in Parquet format

    Args:
        spark (PySpark object): PySpark object
        input_data (string): source json files
        output_data (string): destination Parquet files

    """
    # get filepath to song data file
    song_data = "s3://udacity-dend/song_data/A/A/A/*"
    
    # defined schema
    schema_song = StructType([
    StructField("artist_id", StringType(), True),
    StructField("artist_latitude", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_longitude", DoubleType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("num_songs", LongType(), True),
    StructField("song_id", StringType(), True),
    StructField("title", StringType(), True), 
    StructField("year", LongType(), True)
    ])
    
    # read song data file
    df_song = spark.read.json(song_data, schema=schema_song)

    # extract columns to create songs table
    songs_table = df_song.select(
    df_song.song_id,
    df_song.title,
    df_song.artist_id,
    df_song.year,
    df_song.duration).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").format("parquet")\
    .save(output_data.join("songs/songs_table.parquet"))

    # extract columns to create artists table
    artists_table = df_song.select(
    df_song.artist_id,
    df_song.artist_name.alias("name"),
    df_song.artist_location.alias("location"),
    df_song.artist_latitude.alias("latitude"),
    df_song.artist_longitude.alias("longitude")).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.partitionBy("artist_id").format("parquet")\
    .save(output_data.join("artists/artists_table.parquet"))


def process_log_data(spark, input_data, output_data):
        """ETL - Read Json files (log data), transform it and then save it on disk\
    in Parquet format

    Args:
        spark (PySpark object): PySpark object
        input_data (string): source json files
        output_data (string): destination Parquet files

    """
    # get filepath to log data file
    log_data = input_data.join("log_data/2018/11/*")

    #defined schema
    schema_log = StructType([
    StructField("artist", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("itemInSession", LongType(), True),
    StructField("lastName", StringType(), True),
    StructField("length", DoubleType(), True),
    StructField("level", StringType(), True),
    StructField("location", StringType(), True),
    StructField("method", StringType(), True),
    StructField("page", StringType(), True),
    StructField("registration", DoubleType(), True),
    StructField("sessionId", LongType(), True),
    StructField("song", StringType(), True),
    StructField("status", LongType(), True),
    StructField("ts", LongType(), True),
    StructField("userAgent", StringType(), True),
    StructField("userId", StringType(), True)
    ])
    
    # read log data file
    df_log = spark.read.json(log_data, schema=schema_log)

    # extract columns for users table    
    users_table = df_log.select(
    df_log.userId.alias("user_id"),
    df_log.firstName.alias("first_name"),
    df_log.lastName.alias("last_name"),
    df_log.gender, 
    df_log.level).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.partitionBy("user_id").format("parquet")\
    .save(output_data.join("users/users_table.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_log = df_log.withColumn("datetime", get_datetime(df_log.ts))
    
    # extract columns to create time table
    time_table = df_log.select(
    "timestamp",
    hour("datetime").alias("hour"),
    dayofmonth("datetime").alias("day"),
    weekofyear("datetime").alias("week"),
    month("datetime").alias("month"),
    year("datetime").alias("year"),
    date_format("datetime", "F").alias("weekday"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.format("parquet")\
    .save(output_data.join("time/time_table.parquet"))

    # read in song data to use for songplays table
    df_song = spark.read.parquet(output_data.join("songs/songs_table.parquet"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log.join(
    df_song, 
    (df_log.song == df_song.title) & 
    (df_log.artist == df_song.artist_name) & 
    (df_log.length == df_song.duration),
    'left_outer').select(
    df_log.ts.alias("start_time"), 
    df_log.userId.alias("user_id"), 
    df_log.level, 
    df_song.song_id, 
    df_song.artist_id, 
    df_log.sessionId.alias("session_id"), 
    df_log.location, 
    df_log.userAgent.alias("user_agent")
    ).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").format("parquet")\
    .save(output_data.join("songplays/songplays_table.parquet"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3n://leandro-dend/project4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
