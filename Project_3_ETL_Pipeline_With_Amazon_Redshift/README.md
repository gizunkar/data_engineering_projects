# ETL Pipeline with Amazon Redshift 

## Introduction
The company Sparkify provides a music streaming app to their users. The analytics team wants to understand what songs users are listening to by analyzing their songs and user activity data. 

They keep their data currently in S3 buckets. The goal of this project is to buit an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms them into a star schema for analytic purposes.


## Data Model

We have two source datasets for this project, which are both stored in JSON format. 
 * Song Dataset: It contains metadata about a song and the artist of that song.
 * Log Dataset: It contains logs on user activity on the app.

In our target schema we have 1 fact table and 5 dimension tables. ER Diagram of data model is below


![Image of ER Diagram](https://raw.githubusercontent.com/gizunkar/git_tutorial/master/aws_etl_er_diagram.png)


## Project Files  

1. **dwh.cfg** : It's a configuration file which contains info about Redshift cluster, IAM and S3 bucket. Please make sure that you fill this file correctly before you run codes. 

2. **sql_queries.py**   : It contains all the sql queries of ETL process, which includes DROP, CREATE and INSERT scripts of all tables.

3. **create_tables.py** : It executes the queries to drop all tables in ETL Pipeline and re-create them. 
It should be run first.

   * **Functions**

    * drop_tables(cur, conn): It executes the queries to drop tables.
    * create_tables(cur, conn): It executes the queries to create tables.
    * main(): It gets parameters from config file, connects to Amazon Redshift and calls *drop_tables* and *create_tables* functions respectively.

4. **etl.py** : It executes the queries to make ETL process. 

  * **Functions**

    * load_staging_tables(cur, conn): It executes the queries to take extractions from Amazon S3 and load them into staging tables in Redshift. 
    * insert_tables(cur, conn): It executes queries to insert data from staging tables to target DWH tables. 
    * main(): It gets parameters from config file, connects to Redshift cluster and calls *load_staging_tables* and *insert_tables* functions respectively.

5. **data_quality_and_dashboards.ipyn** : It includes SQLs to check data quality and  visualization for analytic queries. 

## How to Run 

Before you run codes, please make sure that 

* You've created an IAM user has *AdministratorAccess* policy. 
  * You're gonna use its *access key* and *secret key* when you create your clients. 
* You've created an IAM Role which has *AmazonS3ReadOnlyAccess* policy. It makes your Redshift cluster be able to access S3 bucket.
  * You're gonna use its *Role ARN* in dwh.cfg file. 
* Your Redshift cluster is active. 
  * You're gonna use its *Endpoint address* as your host in the config file. 


You should run files in the following order. 

  ``` console
  import create_tables
  create_tables.main()
  import etl
  etl.main()
```



  