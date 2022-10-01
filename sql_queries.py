import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","DWH_ROLE_ARN")
REGION = config.get("CLUSTER","REGION")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# CREATION OF STAGING TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS "staging_events" (
    "artist"        VARCHAR,
    "auth"          VARCHAR,
    "first_name"    VARCHAR,
    "gender"        CHAR(1),
    "iteminSession" INTEGER,
    "last_name"     VARCHAR,
    "length"        FLOAT,
    "level"         VARCHAR,
    "location"      VARCHAR,
    "method"        VARCHAR,
    "page"          VARCHAR,
    "registration"  FLOAT,
    "session_id"    INTEGER,
    "song"          VARCHAR,
    "status"        INTEGER,
    "ts"            TIMESTAMP,
    "user_agent"    VARCHAR,
    "user_id"       INTEGER
);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS "staging_songs" (
    "num_songs"        INTEGER,
    "artist_id"        VARCHAR,
    "artist_latitude"  FLOAT,
    "artist_longitude" FLOAT,
    "artist_location"  VARCHAR,
    "artist_name"      VARCHAR,
    "song_id"          VARCHAR,
    "title"            VARCHAR,
    "duration"         FLOAT,
    "year"             INTEGER
);
""")

# CREATION OF TABLES

# As recommended in the theory videos, we define a sortkey on the fact table
# in the time field, as we usually will do some queries with an order by clause
# --> Useful for columns that are used frequently in sorting like the date dimension and its corresponding foreign key in the fact table

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS "songplays" (
    "songplay_id"    INTEGER IDENTITY (0,1) NOT NULL PRIMARY KEY,
    "start_time"     TIMESTAMP REFERENCES time(start_time) SORTKEY,
    "user_id"        INTEGER REFERENCES users(user_id),
    "level"          VARCHAR NOT NULL,
    "song_id"        VARCHAR REFERENCES songs(song_id),
    "artist_id"      VARCHAR REFERENCES artists(artist_id),
    "session_id"     INTEGER NOT NULL,
    "location"       VARCHAR,
    "user_agent"     VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS "users" (
    "user_id"        INTEGER NOT NULL DISTKEY SORTKEY PRIMARY KEY,
    "first_name"     VARCHAR,
    "last_name"      VARCHAR,
    "gender"         CHAR(1),
    "level"          VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS "songs" (
    "song_id"       VARCHAR NOT NULL PRIMARY KEY,
    "title"         VARCHAR NOT NULL,
    "artist_id"     VARCHAR NOT NULL,
    "year"          INTEGER,
    "duration"      FLOAT
);

""")

artist_table_create = ("""
CREATE TABLE  IF NOT EXISTS  "artists" (
    "artist_id"     VARCHAR NOT NULL PRIMARY KEY,
    "name"          VARCHAR NOT NULL,
    "location"      VARCHAR,
    "latitude"      FLOAT,
    "longitude"     FLOAT
);

""")

time_table_create = ("""
CREATE TABLE  IF NOT EXISTS "time" (
    "start_time"   TIMESTAMP NOT NULL SORTKEY PRIMARY KEY,
    "hour"         INTEGER NOT NULL,
    "day"          INTEGER NOT NULL,
    "week"         INTEGER NOT NULL,
    "month"        INTEGER NOT NULL,
    "year"         INTEGER NOT NULL,
    "weekday"      INTEGER NOT NULL
);

""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
iam_role '{}'
format as JSON {}
REGION {}
timeformat as 'epochmillisecs';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role '{}'
format as JSON 'auto'
REGION {};
""").format(SONG_DATA, DWH_ROLE_ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT se.ts AS start_time,
se.user_id AS user_id,
se.level AS level,
ss.song_id AS song_id,
ss.artist_id AS artist_id,
se.session_id AS session_id,
ss.artist_location AS location,
se.user_agent AS user_agent

FROM staging_songs  ss
JOIN staging_events se ON se.artist=ss.artist_name
AND se.song=ss.title
AND se.page like '%NextSong%'

""")


user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT(se.user_id) AS user_id,
se.first_name AS first_name,
se.last_name AS last_name,
se.gender AS gender,
se.level AS level
FROM staging_events se
WHERE se.page like '%NextSong%'
AND se.user_id NOT IN (SELECT DISTINCT(user_id) FROM users)

""")


song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT DISTINCT(song_id) AS song_id,
title,
artist_id,
year,
duration
FROM staging_songs ss
WHERE song_id NOT IN 
(SELECT DISTINCT(song_id) FROM songs)

""")


artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
SELECT DISTINCT(artist_id) AS artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS latitude,
artist_longitude AS longitude
FROM staging_songs so
WHERE artist_id NOT IN 
(SELECT DISTINCT(artist_id) FROM artists )

""")


time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT(start_time) AS start_time,
extract(hour from start_time) AS hour,
extract(day from start_time) AS day,
extract(week from start_time) AS week,
extract(month from start_time) AS month,
extract(year from start_time) AS year,
extract(weekday from start_time) AS weekday
FROM songplays s
""")

# ANALYTICS QUERIES

analytics_queries = [
    'SELECT COUNT(*) AS TOTAL_STAGING_EVENTS FROM STAGING_EVENTS',
    'SELECT COUNT(*) AS TOTAL_STAGING_SONGS FROM STAGING_SONGS',
    'SELECT COUNT(*) AS TOTAL_SONGPLAYS FROM SONGPLAYS',
    'SELECT COUNT(*) AS TOTAL_USERS FROM USERS',
    'SELECT COUNT(*) AS TOTAL_SONGS FROM SONGS',
    'SELECT COUNT(*) AS TOTAL_ARTISTS FROM ARTISTS',
    'SELECT COUNT(*) AS TOTAL_TIME FROM TIME'    
]

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create  ]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

#copy_table_queries = [staging_events_copy]
copy_table_queries = [staging_events_copy, staging_songs_copy]


insert_table_queries = [songplay_table_insert,user_table_insert,song_table_insert,artist_table_insert,time_table_insert]
