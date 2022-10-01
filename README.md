# DATA ENGINEER MASTER
# Data Warehouse
A Udacity Data Engineer Nanodegree Project

![Alt text](./img/music.jpg?raw=true "A Datawarehouse about music!!")
<br>Foto de <a href="https://unsplash.com/@markusspiske?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Markus Spiske</a> en <a href="https://unsplash.com/es/colecciones/6857718/audiobooks%2C-listening%2C-music?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Unsplash</a>
  

### Table of Contents

1. [Project Motivation](#motivation)
2. [Project Structure](#structure)
3. [Usage](#usage)
4. [Source Datasets](#source_datasets)
5. [Database Model](#database)
6. [File Descriptions](#files)
7. [Dashboards](#dash)
9. [Licensing, Authors, and Acknowledgements](#licensing)
10. [References](#references)


## Project Motivation<a name="motivation"></a> 

There is a startup named Sparkify has grown so much, that they have decided to migrate their data and processes to the cloud, as it gives more elasticity and cost saving.
The data is on a an S3 bucket, in a directory containing JSON logs about the activity of their users using the app.
There is also a folder with metadata of the songs, also in JSON format.

This startup has asked me, as her data engineer, to build an ELT pipeline that extracts all the data from the S3 buckets, stages in Redfhift, and transform that data into dimensional tables, so that the analytics team can find insights into the songs that our users are listening to.
I will test the database and the ETL pipeline and execute some queries to compare the results with the expected results.


> * Question 1: What are the songs that users are listening to?
> * Question 2: What are the artists that users are listening to?
> * Question 3: How many users do we have?
> * Question 4: How many users do we have by levels (free/paid)?
> * Question 5: How many users do we have by gender?
> * Question 6: What are the 5 location where users listen the most?

They don't have an easy way to query their data that is stored in a directory of JSON files with the logs on user activity on the app. They have also a directory with JSON metadata on the songs in their app.

The goal of this project are:
> * Creating a Redshift datawarehouse with tables specifically designed to optimize queries on song play analysis. 
> * Create a datawarehouse, defining fact and dimension tables for a star schema
> * Design and implement the ETL pipeline for the analysis. This pipeline transfers data from files in two directories into some tables 
> * Use Python and SQL

## Project Structure<a name="structure"></a>

This is the structure of the project:

![Alt text](./img/tree_dwh.png?raw=true "tree structure of the project")


## Usage <a name="usage"></a>

The first step is to include the AWS Access Key and Secret key into the dwh.cfg configuration file

The second step is to create the Redshift cluster on AWS:
> * python create_redshift.py

The third step is creating the tables for the database and creating a conection to a Postgres database named sparkifydb.
To do so, run the following command in a terminal at the root folder:

> * python create_tables.py

Once the previous command has been executed, we need to start reading the source data and filling the database tables 
with the source data in the right way, that is, we need to start executing an ETL process.For that you can use:

> * python etl.py


Don't forget to delete all the AWS services if you don't want to have an extra cost!!!!
>* python delete_redshift.py

Here some screenshots of the scripts running:
![Alt text](./img/create.png?raw=true "script1")

Here a screenshoft of the Redshift Editor
![Alt text](./img/redshift.png?raw=true "redshift")

<br>

## Source Datasets <a name="source_datasets"></a>

The files included for the analysis are two datasets:

> * Song Dataset       - This dataset is a subset of the Million Song Dataset () and each file is in JSON format and contains metadata about a song and artist of that song. The files are partitioned by the first three letters of each songs 's track. 
> * Log Dataset        - This dataset consists of log files in JSON format generated by an event simulator based on the songs in the previous dataset. That is to simulate activity logs of users of a music streaming app.

## Database Model <a name="database"></a>

The schema of the Database is a star schema and can be described with the following diagram:
![Alt text](./img/DWH_Star_schema.png?raw=true "Database_model")

## File Descriptions <a name="files"></a>

These are the python scripts that create the databases schema and all the queries:

1. create_tables.py: Prepare all the workspace with a new schema and create all tables <br>
2. etl.py: Read the Json logs and metadata and load that info into the recently created tables
3. sql_queries.py: This script contains all the queries

## Dashboarding<a name="dash"></a> 

During the project, I have also added an EDA (Exploratory Data Analysis) and I include here some of the graphics for a better understanding of this dataset
<br>

 <br>Number of users
![Alt text](./img/nusers.png?raw=true "Number of Users")

<br>Graphic of Users by Level
![Alt text](./img/usersbylevel.png?raw=true "Users by Level")

 <br>Graphic of Gender Distribution
![Alt text](./img/usersbygender.png?raw=true "Gender Distribution")

 <br>Graphic of Users by American State
![Alt text](./img/usersbylocation.png?raw=true "Users by State")

 <br>Graphic of 5 Top Artist
![Alt text](./img/top_artists.png?raw=true "5 Top Artist")

## Licensing, Authors, Acknowledgements<a name="licensing"></a>

Must give credit to Udacity for collecting data and because there are some pieces of code taken out from the Data Engineer Nanodegree classrooms. 
Also credits to Udacity Knowledge, where there is important information to develop the project
And to Stackoverflow as it has been a useful source to solve some errors

## References <a name="references"></a>
 [Implementing Data Warehouses on AWS](https://learn.udacity.com/nanodegrees/nd027/parts/cd0055/lessons/19927dab-ffc2-4123-aec4-3bd3cb034a16/concepts/afe30bab-0227-4fe9-b630-b79957f59a75) <br>
 [Udacity Knowledge](https://knowledge.udacity.com/) <br>
 [StackOverflow](https://stackoverflow.com/) <br>
 
