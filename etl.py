from pyspark.sql import SparkSession

def create_spark_session():
    return spark

def process_song_data(spark, input_data, output_data):

def process_log_data(spark, input_data, output_data):

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3n://leandro-dend/project4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()