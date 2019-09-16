from pyspark.sql import SparkSession
import configparser
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
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

def process_log_data(spark, input_data, output_data):

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
	
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3n://leandro-dend/project4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()