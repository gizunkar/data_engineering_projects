import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.functions import to_date, unix_timestamp, to_timestamp, date_format
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.functions import row_number
from pyspark.sql.functions import desc
from pyspark.sql.window import Window


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    #get full path 
    song_data=input_data+"song-data/song_data/*/*/*/*.json"
    
    # read song data file
    song_data_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_columns=['song_id', 'title','artist_id', 'year', 'duration' ]
    songs_table_df = song_data_df.select(*songs_columns).dropDuplicates()
    
    print(songs_table_df.count())
    

    # write songs table to parquet files partitioned by year and artist
    songs_table_df.write.partitionBy("year", "artist_id").parquet(output_data+'songs_table/', mode='overwrite')
    
    # extract columns to create artists table
    artists_columns=['artist_id', 'artist_name','artist_location', 'artist_latitude', 'artist_longitude' ]
    artists_table_df = song_data_df.select(*artists_columns).dropDuplicates()

    print(artists_table_df.count())
    
    # write artists table to parquet files
    artists_table_df.coalesce(5).write.parquet(output_data+"artists_table/", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    
    # get filepath to log data file
    log_data_path =input_data+"log-data/*.json"

    # read log data file
    log_data_df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    log_data_df = log_data_df.filter((log_data_df.page== 'NextSong'))
    
    ####
    #convert unixtimestamp
    log_data_df= log_data_df.withColumn('timestamp', F.to_timestamp(F.date_format((log_data_df.ts /1000) 
                            .cast(dataType=T.TimestampType()), "yyyy-MM-dd HH:MM:ss z"), "yyyy-MM-dd HH:MM:ss z"))


    # extract columns for users table
    
    #log data has duplicated UserIds.But we want to only one row for each user. 
    ## So we get the most recent record of the user by using a row_number. 
    
    user_columns= ['userId', 'firstName', 'lastName', 'gender', 'level', 'timestamp']
    
    ## remove duplicates using row_number.
    user_df_rn = log_data_df.select(*user_columns)\
            .withColumn('row_num', F.row_number().over(Window.partitionBy("userId").orderBy(F.desc("timestamp"))))\
    
    users_table_df = user_df_rn.filter((user_df_rn.row_num)==1).select(*user_columns[0:-1])

    
    # write users table to parquet files
    users_table_df.coalesce(5).write.parquet(output_data+'users_table/', mode='overwrite')

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table_df= log_data_df.select( log_data_df.timestamp.alias('start_time'), 
                                      F.hour(log_data_df.timestamp).alias('hour'),
                                      F.dayofmonth(log_data_df.timestamp).alias('day'),
                                      F.weekofyear(log_data_df.timestamp).alias('week'),
                                      F.month(log_data_df.timestamp).alias('month') , 
                                      F.year(log_data_df.timestamp).alias('year'), 
                                      F.dayofweek(log_data_df.timestamp).alias('weekday')).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table_df.write.partitionBy("year","month").parquet(output_data+'time_table/', mode='overwrite')


    # ???- read in song data to use for songplays table
    #get full path 
    song_data=input_data+'song-data/song_data/*/*/*/*.json'    
    # read song data file
    song_data_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    
    songplays_table_df= log_data_df.join\
                        (song_data_df, (log_data_df.artist == song_data_df.artist_name) 
                                         & (log_data_df.song== song_data_df.title)
                                         & ( log_data_df.length== song_data_df.duration), how='inner')\
                        .select(log_data_df.timestamp , log_data_df.userId, 
                                log_data_df.level, song_data_df.song_id, song_data_df.artist_id, 
                                log_data_df.sessionId, log_data_df.location, 
                                log_data_df.userAgent  )

    # write songplays table to parquet files partitioned by year and month
    songplays_table_df.write.partitionBy("year","month").parquet(output_data+'songplays_table/', mode='overwrite')
    
    print(users_table_df.count())
    print(time_table_df.count())
    print(songplays_table_df.count())


def main():
    spark = create_spark_session()
    input_data = '/home/gizem/Desktop/project datalake/source_data/'#"s3a://udacity-dend/"
    output_data = "/home/gizem/Desktop/project datalake/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
