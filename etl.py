import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
      The create_spark_session function creates a new spark session or retrieves the previous spark session if already created.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
      The process_song_data function creates a new spark dataframe from the song data json files stored in the S3 bucket, extracts the required columns and converts the   
      created table into parquet format. Schema-On-Read feature of spark is used.
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song-data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("dfview")

    # extract columns to create songs table
    songs_table = spark.sql('''
            SELECT DISTINCT
                song_id, 
                title, 
                artist_id, 
                year, 
                duration
            FROM dfview
            WHERE song_id IS NOT NULL''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data + '/songs/songs_table.parquet')

    # extract columns to create artists table
    artists_table = spark.sql('''
            SELECT DISTINCT
                artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS lattitude,
                artist_longitude AS longitude
            FROM dfview
            WHERE artist_id IS NOT NULL
            ''')
    artists_table.head(5)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + '/artists/artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    """
      The process_log_data function creates a new spark dataframe from the logs json files stored in the S3 bucket, extracts the required columns and converts the   
      created table into parquet format. Schema-On-Read feature of spark is used.
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("dfview")

    # extract columns for users table    
    users_table = spark.sql('''
            SELECT DISTINCT
                userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
            FROM dfview
            WHERE userId IS NOT NULL
            ''')
    users_table.head(5)
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "/users/users_table.parquet")
    

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn('date', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour('date').alias('hour'),
                           dayofmonth('date').alias('day'),
                           weekofyear('date').alias('week'),
                           month('date').alias('month'),
                           year('date').alias('year'),
                           date_format('date','E').alias('weekday'))
    time_table.createOrReplaceTempView("TimeView")

    time_table.head(5)
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + '/time/time_table.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song-data/A/A/A/*.json'))
    song_df.head(5)
    song_df.createOrReplaceTempView("songView")
    
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        SELECT DISTINCT
            tt.start_time,
            tt.year,
            tt.month,
            se.userId AS user_id,
            se.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            se.sessionId AS session_id,
            se.location AS location,
            se.userAgent AS user_agent
        FROM dfview se
        JOIN songView ss ON se.song = ss.title
        JOIN TimeView tt ON se.ts = tt.start_time
        AND se.artist = ss.artist_name
        AND se.length = ss.duration''')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + '/songplays/songplays_table.parquet')


def main():
    """
      The main function -
      I. Creates a spark session
     II. Parses the song files stored in the S3 Bucket
    III. Parses the log files stored in the S3 Bucket
    """
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://my-udacity-course-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
