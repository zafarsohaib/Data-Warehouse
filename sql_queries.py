import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    row_id int IDENTITY (1,1), 
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length decimal,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId int,
    song varchar,
    status int,
    ts timestamp,
    userAgent varchar,
    userId int,
CONSTRAINT se_uq UNIQUE (ts, userId, sessionId));
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    row_id int IDENTITY (1,1), 
    num_songs int,
    artist_id varchar,
    artist_latitude decimal, 
    artist_longitude decimal, 
    artist_location varchar, 
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration decimal,
    year int,
CONSTRAINT ss_uq UNIQUE (artist_id, song_id)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int  IDENTITY (1,1), 
    start_time  timestamp   NOT NULL SORTKEY, 
    user_id     int         NOT NULL DISTKEY, 
    level       varchar, 
    song_id     varchar     NOT NULL, 
    artist_id   varchar     NOT NULL, 
    session_id  int         NOT NULL, 
    location    varchar, 
    user_agent  varchar, 
CONSTRAINT songplay UNIQUE (start_time, user_id, session_id), 
PRIMARY KEY (songplay_id),
FOREIGN KEY (start_time) REFERENCES time (start_time),
FOREIGN KEY (user_id) REFERENCES users (user_id),
FOREIGN KEY (song_id) REFERENCES songs (song_id),
FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    row_id int IDENTITY (1,1), 
    user_id    int       NOT NULL SORTKEY DISTKEY, 
    first_name varchar, 
    last_name  varchar, 
    gender     varchar, 
    level      varchar,
CONSTRAINT user_uq UNIQUE (user_id), 
PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
    row_id int IDENTITY (1,1),
    song_id varchar NOT NULL SORTKEY DISTKEY, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration numeric,
CONSTRAINT song UNIQUE (song_id), 
PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    row_id int IDENTITY (1,1),
    artist_id varchar NOT NULL SORTKEY DISTKEY, 
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric,
CONSTRAINT artist UNIQUE (artist_id), 
PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    row_id int IDENTITY (1,1),
    start_time timestamp NOT NULL SORTKEY DISTKEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int,
CONSTRAINT st UNIQUE (start_time), 
PRIMARY KEY (start_time)
);
""")

# STAGING TABLES
# Delete is used with every insert to avoid duplicate row insertion, because Redshift doesn't enforce unique consraints.

staging_events_copy = ("""
COPY staging_events 
    from {}
    iam_role {}
    region 'us-west-2'
    format as json {}
    timeformat 'epochmillisecs'
    BLANKSASNULL EMPTYASNULL;
DELETE FROM staging_events WHERE row_id in (
    SELECT row_id FROM ( SELECT row_id,
                        row_number() over (partition by ts, userId, sessionId) as rown
                        FROM staging_events) a 
                        where rown > 1);
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs 
    from {}
    iam_role {}
    region 'us-west-2'
    format as json 'auto'
    BLANKSASNULL EMPTYASNULL;
DELETE FROM staging_songs WHERE row_id in (
    SELECT row_id FROM ( SELECT row_id,
                        row_number() over (partition by artist_id, song_id) as rown
                        FROM staging_songs) a 
                        where rown > 1);
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT se.ts, se.userId, se.level, ss.song_id, 
    ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se
    inner join staging_songs ss on (ss.artist_name = se.artist AND ss.title = se.song)
    where se.page = 'NextSong';
DELETE FROM songplays WHERE songplay_id in (
    SELECT songplay_id FROM ( SELECT songplay_id,
                            row_number() over (partition by start_time, user_id, session_id) as rown
                            FROM songplays) a 
                            where rown > 1);
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM ( SELECT userId, firstName, lastName, gender, level,
                row_number() over (partition by userId order by ts desc) as rown
                from staging_events
                WHERE page = 'NextSong' AND userId is not NULL)
        WHERE rown=1;
DELETE FROM users WHERE row_id in (
    SELECT row_id FROM ( SELECT row_id,
                        row_number() over (partition by user_id) as rown
                        FROM users) a 
                        where rown > 1);
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM ( SELECT song_id, title, artist_id, year, duration,
                row_number() over (partition by song_id) as rown
                from staging_songs
                WHERE song_id is not NULL)
        WHERE rown=1;
DELETE FROM songs WHERE row_id in (
    SELECT row_id FROM ( SELECT row_id,
                        row_number() over (partition by song_id) as rown
                        FROM songs) a 
                        where rown > 1);
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM ( SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
                row_number() over (partition by artist_id order by artist_name) as rown
                from staging_songs
                WHERE artist_id is not NULL)
        WHERE rown=1;
DELETE FROM artists WHERE row_id in (
    SELECT row_id FROM ( SELECT row_id,
                        row_number() over (partition by artist_id) as rown
                        FROM artists) a 
                        where rown > 1);
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts,
                    EXTRACT(hour from ts),
                    EXTRACT(day from ts),
                    EXTRACT(week from ts),
                    EXTRACT(month from ts),
                    EXTRACT(year from ts),
                    EXTRACT(weekday from ts)
    FROM staging_events 
    where page = 'NextSong' and ts is not NULL;
DELETE FROM time WHERE row_id in (
    SELECT row_id FROM ( SELECT row_id,
                        row_number() over (partition by start_time) as rown
                        FROM time) a 
                        where rown > 1);
""")

# ANALYTICS QUERY LIST
query1 = ("""
SELECT u.first_name, u.last_name FROM songplays sp 
inner join users u on u.user_id=sp.user_id 
inner join songs s on s.song_id=sp.song_id 
where s.title='Setanta matins';
""")

query2 = ("""
SELECT first_name, last_name FROM users 
where level='paid' 
order by first_name limit 10;
""")

query3 = ("""
SELECT t.day || '-' || t.month  || '-' || t.year as LAST_ACCCESS_DATE FROM songplays sp 
inner join time t on t.start_time = sp.start_time 
inner join users u on u.user_id=sp.user_id 
where u.first_name='Aleena' 
order by sp.start_time desc limit 1;
""")

query4 = ("""
SELECT count(*) from artists;
""")
query4b = ("""
SELECT count(distinct artist_id) from artists;
""")

query5 = ("""
SELECT count(*) from users;
""")
query5b = ("""
SELECT count(distinct user_id) from users;
""")

query6 = ("""
SELECT count(*) from songs;
""")
query6b = ("""
SELECT count(distinct song_id) from songs;
""")

query7 = ("""
SELECT count(*) from time;
""")
query7b = ("""
SELECT count(distinct start_time) from time;
""")

query8 = ("""
SELECT count(*) from songplays;
""")
query8b = ("""
SELECT count(distinct songplay_id) from songplays;
""")

query9 = ("""
SELECT count(*) from staging_events;
""")

query10 = ("""
SELECT count(*) from staging_songs;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_queries = [query1, query2, query3, query4, query4b, query5, query5b, query6, query6b, query7, query7b, query8, query8b, query9, query10]
