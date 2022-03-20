import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        iteminSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userid int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id int identity(1, 1),
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
    CREATE TABLE IF NOT EXISTS users (
        user_id int,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id varchar,
        title varchar,
        artist_id varchar,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id varchar,
        name varchar,
        location varchar,
        lattitude varchar,
        longitude varchar
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES
# The following two queries were created with the help from https://github.com/brfulu/redshift-data-modeling.
staging_events_copy = ("""
    copy staging_events from {} 
    iam_role {}
    json {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {} 
    iam_role {}
    format as json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
    SELECT 
    TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 Second ',
    event.userId,
    event.level,
    song.song_id,
    song.artist_id,
    event.sessionId,
    event.location,
    event.userAgent
    FROM
    staging_events event
    INNER JOIN
    staging_songs song
    ON
    event.song = song.title
""")

user_table_insert = ("""
    INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
    )
    SELECT
    userId,
    firstName,
    lastName,
    gender,
    level
    FROM
    staging_events
""")

song_table_insert = ("""
    INSERT INTO song (
    song_id,
    title,
    artist_id,
    year,
    duration
    )
    SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM
    staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artist (
    artist_id,
    name,
    location,
    lattitude,
    longitude
    )
    SELECT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM
    staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
    )
    SELECT
    start_time,
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
    FROM
    songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
