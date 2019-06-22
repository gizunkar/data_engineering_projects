import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS  artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create=("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist VARCHAR ,
                                auth VARCHAR,
                                first_name VARCHAR,
                                gender CHAR(1),
                                item_session NUMERIC,
                                last_name VARCHAR,
                                length NUMERIC,
                                level VARCHAR,
                                location VARCHAR,
                                method VARCHAR,
                                page VARCHAR,
                                registration NUMERIC,
                                session_id NUMERIC,
                                song VARCHAR,
                                status NUMERIC,
                                ts timestamp,
                                user_agent VARCHAR,
                                user_id NUMERIC
                            )
                            """)

staging_songs_table_create = ("""CREATE  TABLE IF NOT EXISTS staging_songs(
                                num_songs NUMERIC,
                                artist_id VARCHAR,
                                artist_latitude NUMERIC,
                                artist_longitude NUMERIC,
                                artist_location VARCHAR,
                                artist_name VARCHAR distkey,
                                song_id VARCHAR,
                                title VARCHAR,
                                duration NUMERIC,
                                year NUMERIC
                            )
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY sortkey, 
                            start_time timestamp, 
                            user_id NUMERIC NOT NULL, 
                            level VARCHAR, 
                            song_id VARCHAR NOT NULL ,  
                            artist_id VARCHAR NOT NULL,
                            session_id VARCHAR NOT NULL, 
                            location VARCHAR, 
                            user_agent VARCHAR
                            )  
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id NUMERIC PRIMARY KEY sortkey,
                        first_name VARCHAR ,
                        last_name VARCHAR ,
                        gender VARCHAR,
                        level VARCHAR
                        )
                        """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR PRIMARY KEY sortkey,
                        title VARCHAR,
                        artist_id VARCHAR NOT NULL,
                        year NUMERIC,
                        duration NUMERIC
                    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                            artist_id VARCHAR PRIMARY KEY sortkey,
                            name VARCHAR,
                            location VARCHAR,
                            lattitude NUMERIC,
                            longitude NUMERIC
                            )
                            """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp PRIMARY KEY sortkey,
                            hour NUMERIC,
                            day NUMERIC,
                            week NUMERIC,
                            month NUMERIC,
                            year NUMERIC,
                            weekday VARCHAR
                            )
                            """)
# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          compupdate off region 'us-west-2'
                          json {}
                          TIMEFORMAT as 'epochmillisecs';
                       """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs 
                          from {}
                          iam_role {}
                          compupdate off region 'us-west-2'
                          JSON 'auto' truncatecolumns;
                       """).format(SONG_DATA, IAM_ROLE)


# FINAL TABLES

songplay_table_insert =(""" INSERT INTO songplays 
                            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent ) 
                            SELECT e.ts , e.user_id, 
                            e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent  
                            FROM staging_events e
                            JOIN staging_songs s ON 
                            e.artist= s.artist_name 
                            AND e.song= s.title
                            AND e.length= s.duration
                            WHERE e.page='NextSong'  """)


user_table_insert = ("""INSERT INTO users (user_id,first_name, last_name, gender, level)
                        SELECT user_id, first_name,last_name, gender, level FROM 
                        (
                        SELECT ROW_NUMBER() OVER(PARTITION BY user_id 
                        ORDER BY se.ts DESC) rn,
                        user_id, first_name,last_name, gender, level, ts
                        FROM staging_events se 
                        WHERE user_id is not null 
                        ) WHERE rn =1                      
                    """)

song_table_insert = (""" INSERT INTO songs(song_id ,title, artist_id ,year,duration )
                        SELECT DISTINCT song_id, title, artist_id, year, duration 
                        FROM staging_songs
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                            SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            FROM 
                            ( SELECT ROW_NUMBER() 
                              OVER(PARTITION BY artist_id  ORDER BY artist_latitude, artist_longitude DESC) rn,
                                   artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                              FROM staging_songs 
                              WHERE artist_id is not null 
                            ) WHERE rn =1    
                    """)


time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                            SELECT start_time, extract(hour from start_time), extract(day from start_time),
                            extract(week from start_time), extract(month from start_time), extract(year from start_time),
                            extract(weekday from start_time) 
                            FROM songplays
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
