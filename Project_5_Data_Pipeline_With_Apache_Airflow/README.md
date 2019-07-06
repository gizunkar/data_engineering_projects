# Data Pipelines With Apache Airflow

The company Sparkify provides a music streaming app to their users. The analytics team wants to understand what songs users are listening to by analyzing their songs and user activity data. 

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


The goal of this project is to built an ETL pipeline with Apache Airflow. 
## Data Model

We have two source datasets for this project, which are both stored as JSON files in a public S3 bucket. 
 * Song Dataset: It contains metadata about songs and artists of songs and is stored in s3://udacity-dend/song_data
 * Log Dataset: It contains logs on user activity on the app and is stored in s3://udacity-dend/log_data

In this project the task is first to read these 2 files from S3 and load into two staging tables in Redshift (*staging_events* and *staging_song*), after that to create 4 dimension tables (*artist*, *songs*, *users*, *time*) and 1 fact table(*songplays*) by using staging tables. 

The graph of pipeline is below

![Image of airflow graph](https://raw.githubusercontent.com/gizunkar/data_engineering_projects/master/Project_5_Data_Pipeline_With_Apache_Airflow/img/sparkify_pipeline.png)


To complete the project, four different operators are builded which are shortly explained below.

 * **Stage Operator:** is able to load any JSON and CSV formatted files from S3 to Amazon Redshift.

 * **Fact and Dimension Operators:** these operators take as input a SQL statements and run these queries against the target database. 

 Dimension loads are done with the truncate-insert pattern. On the other hand the fact table is loaded with append pattern. 

 * **Data Quality Operator:** It is used to run checks on the data itself. 



## Project Files  

1. /sparkify_project/dags/sparkify_pipeline.py : It's the file where DAG is created and dependencies are defined. 

2. /sparkify_project/plugins/helpers/sql_queries.py : It's a provided SQL helper class to run data transformations.

3. /sparkify_project/plugins/operators/stage_redshift.py : It's the file where StageToRedshiftOperator is created.  

4. /sparkify_project/plugins/operators/load_dimension.py : It's the file where LoadDimensionOperator is created.

5. /sparkify_project/plugins/operators/load_fact.py : It's the file where LoadFactOperator is created.

6. /sparkify_project/plugins/operators/data_quality.py: It's the file where DataQualityOperator is created. 

7. sparkify_projects/create_tables.sql : It's the file where all create scripts are stored. 


## How to Run 

Before run the DAG, please make sure that you have created following 3 variables and 2 connections on airflow UI. 

 * Variables : 

|Key			| Value		
| :---         	|        ---:  |			
| s3_bucket    	| udacity-dend |
| s3_log_prefix	| log_data	   |
| s3_song_prefix| song_data	   |

 * Connection

	 * Conn Id: Enter aws_credentials.
	 * Conn Type: Enter Amazon Web Services.
	 * Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
	 * Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

* Connection 

	 * Conn Id: Enter redshift.
	 * Conn Type: Enter Postgres.
	 * Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
	 * Schema: Enter dev. This is the Redshift database you want to connect to.
	 * Login: Enter awsuser.
	 * Password: Enter the password you created when launching your Redshift cluster.
	 * Port: Enter 5439.

Gizem Unkar