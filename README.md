
# Sparkify Datalake
> Python Spark pipelines.

##Introduction
This project for the Udacity Data Engineering Nano Degree (DEND) course. The Pyspark framework was used to read disk files and perform ETL. The tables created were 1 Fact Table (songplays_table) and 4 Dimension Tables (users_table, songs_table, artists_table and time_table) for Sparkify's Datalake. The Fact Table represents the relationship between users and songs \
listened to. The Fact Table, together with the Dimension Tables allow Sparkify (the company) to analyze user behaviour. \
Examples include which users listen to what music, favorite artists, songs, total listening time, etc.
##Description of the Fact Table and Dimensions Tables (Schema Design)
###Fact Table 
songplay_table - contains the vast majority of information. This is essentially the log information from the log files \
subsetted to just the records that are pertinent to songs being listened to (NextSong). This subsetting on NextSong \
ripples down to all of the dimension tables that are derived from the log file. The songplay table contains \
(start_time, user_id, level, song_id, artist_id, session_id, location, and user_agent).
###Dimension Tables
songs_table – This is derived from the song files. Each row in the song file is parsed to create a song row that \
contains song_id, title, artist_id, year, duration. Year is stored as integer and duration as float. 
users_table  - This is derived from the log file. Each row in the log file is parsed and stores user_id, first_name,  \
last_name, gender,  and level
artist_table  - This is derived from the log file records. Each row in the log file is parsed and stores artist_id, \
name, location, latitude, and longitude.
time_table  - This is derived from the log file records. Each row in the log file is parsed and stores start_time, \
hour, day, week, month, year, weekday.
## Usage of the Sparkify Datalake 
Data is read from AWS S3 files and ETLd with Python Spark. The 4 Dimension Tables are joined with the Fact Table to produce\
query results. These tables are saved back to S3 using Parquet file format as a Datalake. An example would be the artist_table \
being joined with songplay table and answering the question who is the most popular artist in terms of number of \
song_plays. A simple join between the songplay (Fact Table) and the artists (Dimension Table) joined on artist_id \
would yield the name of the artists and a count sorted DESC, thus answering this and many other important analytical \
questions.  

## Installing / Getting started

Developed with Python version 3.7

Dependency library:
>pyspark
configparser
datetime


```shell
pip install pyspark
```

### Initial Configuration

Pyspark framework database is required.
S3 stored JSON files are required as input data.
Define AWS infrastructure (EMR).

## Developing

N/A

### Building

N/A

### Deploying / Publishing

N/A

## Features

etl.ipynb -> Read json data from S3. Use Spark for ETL. Write Parquet files do S3. sql_queries.py\
dwh.cfg file is required.

dwh.cfg -> Configuration data file.

## Configuration

N/A

## Contributing

If you'd like to contribute, please fork the repository and use a feature
branch. Pull requests are warmly welcome.

## Links

- Udacity site: http://udacity.com/
- Developed by Leandro Queiroz: lerqueiroz@gmail.com

## Licensing

The code in this project is licensed under GPL license
09/2019

