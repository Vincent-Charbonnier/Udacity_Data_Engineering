# Project 1 - Data Modeling with Cassandra

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analysis team is particularly interested in understanding what songs users are listening to. 
Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.
They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. 
The Jupiter Notebooks will be used to create a database for this analysis and to test the database by running the following queries given by the analytics team from Sparkify to create the results:
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

## Datasets
For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Project Template
In addition to the data files, the project repository includes 7 files:
	- you will process the ```event_datafile_new.csv``` dataset to create a denormalized dataset
	- you will model the data tables keeping in mind the queries you need to run
	- you have been provided queries that you will need to model your data tables for
	- you will load the data into tables you create in Apache Cassandra and run your queries
1. Project_1B_ Project_Template.ipynb: process the event_datafile_new.csv dataset to create a denormalized dataset, model the data tables, load the data into tables and run required queries
2. README.md: provides the project's description.
3. event_datafile_new.csv: Dataset that results of the processed fata from the above Jupyter Notebook.

The event_datafile_new.csv contains the following columns:
1. artist,
2. first name of user,
3. gender of user,
4. item number in session,
5. last name of user
6. length of the song
7. level (paid or free song)
8. location of the user
9. session id
10. song title
11. user id
	
The image below is a screenshot of what the denormalized data should appear like in the event_datafile_new.csv after the code above is run:
![image](https://github.com/Vincent-Charbonnier/Udacity_Data_Engineering/raw/af9b536f66622b1048fe0e176026ad54a408d30e/Data%20Modeling/Project%202%20-%20Data%20Modeling%20with%20Apache%20Cassandra/images/image_event_datafile_new.jpg)
