import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists "staging_events" (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar(max),
        method varchar,
        page varchar,
        registration numeric,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    create table if not exists "staging_songs" (
        artist_id varchar,
        artist_latitude numeric,
        artist_location varchar,
        artist_longitude numeric,
        artist_name varchar,
        duration numeric,
        num_songs int,
        song_id varchar,
        title varchar,
        year int
    )
""")

songplay_table_create = ("""
    create table if not exists "songplay" (
        songplay_id int identity(0,1) primary key,
        start_time timestamp, 
        user_id int, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar
    )
""")

user_table_create = ("""
    create table if not exists "users" (
        user_id int primary key, 
        first_name varchar,
        last_name varchar, 
        gender varchar, 
        level varchar
    )
""")

song_table_create = ("""
    create table if not exists "songs" (
        song_id varchar primary key,
        title varchar, 
        artist_id varchar,
        year int, 
        duration numeric
    )
""")

artist_table_create = ("""
    create table if not exists "artists" (
        artist_id varchar primary key, 
        name varchar,
        location varchar, 
        latitude numeric, 
        longitude numeric
    )
""")

time_table_create = ("""
    create table if not exists "time" (
        start_time timestamp primary key, 
        hour int, 
        day int, 
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    copy staging_events from '{config["S3"]["LOG_DATA"]}' 
    credentials 'aws_iam_role={config["IAM_ROLE"]["ARN"]}' 
    format as json '{config["S3"]["LOG_JSONPATH"]}'
    timeformat 'epochmillisecs'
    region 'us-west-2';
""")

staging_song_copy_small = (f"""
    copy staging_songs from 's3://udacity-dend/song_data/A/A/A' 
    credentials 'aws_iam_role={config["IAM_ROLE"]["ARN"]}' 
    format as json 'auto'
    region 'us-west-2';   
""")

staging_songs_copy = (f"""
    copy staging_songs from '{config["S3"]["SONG_DATA"]}'
    credentials 'aws_iam_role={config["IAM_ROLE"]["ARN"]}' 
    format as json 'auto'
    region 'us-west-2';                  
""")

# FINAL TABLES
songplay_table_insert = ("""
    insert into songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select  se.ts
            , se.userId
            , se.level
            , ss.song_id
            , ss.artist_id
            , se.sessionId
            , se.location
            , se.userAgent
    from staging_events se
    left join staging_songs ss 
    on se.artist = ss.artist_name and se.song = ss.title
    where se.page = 'NextSong'
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select  distinct userId
            , firstName
            , lastName
            , gender
            , level
    from staging_events
    where page = 'NextSong'
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    select  distinct song_id
            , title
            , artist_id
            , year
            , duration
    from staging_songs
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    select  distinct artist_id
            , artist_name
            , artist_location
            , artist_latitude
            , artist_longitude
    from staging_songs
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    select  distinct ts
            , extract(hour from ts)
            , extract(day from ts)
            , extract(week from ts)
            , extract(month from ts)
            , extract(year from ts)
            , extract(weekday from ts)
    from staging_events
    where ts is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
copy_table_queries_small = [staging_events_copy, staging_song_copy_small]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
