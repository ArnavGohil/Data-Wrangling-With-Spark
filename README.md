# Project 4

### PROJECT SUMMARY
In this project, an ETL pipeline was developed that extracts their data from AWS S3 Bucket, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This allows the analytics team to continue finding insights in what songs the users are listening to. 
The data resides in S3, in a directory of JSON logs on user activity on a Song app, as well as a directory with JSON metadata on the songs in the app. 

### PROJECT SCHEMA
Star Schema was used for the development of the tables.
4 Dimension tables and a single fact table was used.
The ER Diagram for the project -

![ERD](ERD.png)

### PROJECT FILES

1. ```etl.py``` - This is the script for creating the main ETL Pipeline. It takes data from the S3 bucket files, converts the JSON file to Spark DataFrames, converts it into the required facts and dimensions table and finally converts the table into parquet format and stores it back into a S3 bucket. To execute this file, run the following command in Terminal - ``` python etl.py ```

2. ```dl.cfg``` - Contains the AWS credentials.

:sparkles:
