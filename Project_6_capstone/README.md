# Udacity Capstone Project 

In this project, I designed and implemented a dimensional data model from yelp's public data set for analytic purposes. I developed the model with Pyspark, stored the dimensional model in Amazon s3 and Redshift, submitted spark application on Amazon EMR and scheduled the pipeline with Apache Airflow. 

[Link to yelp's dataset page](https://www.yelp.com/dataset)



## Data Model

I used 3 JSON files as source and I developed a dimensional model to be able to analyze business reviews.
 * business.json 
 * user.json
 * review.json
[Link to source data dictionary](/home/gizem/Desktop/udend-capstone/documentation/data_dictionary.md)

In my data model, there is one fact table (business reviews) and 4 dimensions (users, category, ambiance, state). 

[Link to target data dictionary](/home/gizem/Desktop/udend-capstone/dimensional_model_data_dictionary.md)

The ER diagram of data model:

![Image of ER Diagram graph](https://github.com/gizunkar/data_engineering_projects/blob/master/Project_6_capstone/img/dag.png?raw=true)


## ETL 

I read the source data files from a S3 bucket, after cleaning and transformation I store fact and dimensions in another S3 bucket for permanent storage and also copy these files into Redshift to run analytic queries and aggregations for visualization. I orcahstate this pipeline with Apache Airflow. I used Livy to submit spark application via Airflow. 

The graph of pipeline is below


![Image of airflow graph](/home/gizem/Desktop/udend-capstone/documentation/img/airflow_dag.png)



## Project Files  

1. /yelp_data_eng/dags/yelp_dwh_pipeline.py : It's the file where DAG is created and dependencies are defined. 

2. /yelp_data_eng/yelp_dwh.py : The spark application. There's a copy of this file in a public S3 bucket and this S3 path is given as parameter in spark submit operator.

6. /yelp_data_eng/plugins/operators/data_quality.py: It's the file where DataQualityOperator is created. 

7. /yelp_data_eng/plugins/operators/ssh_spark_submit_operator.py: It's the file where SSHSparkSubmitOperator is created. This operator submits a spark application on EMR using ssh connection.

8. /yelp_data_eng/create_tables.sql : It's the file where all create scripts are stored. 


## How to Run 
To be able to run the Dag you have to have an active Redshift cluster and an active EMR cluster. 
EMR cluster should have Spark and Hadoop.

Also before run the DAG, please make sure that you have created necessary variables and connections on airflow UI. 

 * Variable (for your s3 bucket where you store the input data)

 	* Key: Enter s3_input_path 
 	* Value: Enter your s3 path (you can enter s3a://yelp-input)

 * Variable (for your s3 bucket where you store the output data)

 	* Key: Enter s3_output_path 
 	* Value: Enter your s3 path 


 * Connection (for aws)

	 * Conn Id: Enter aws_credentials.
	 * Conn Type: Enter Amazon Web Services.
	 * Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
	 * Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

* Connection (for redshift)

	 * Conn Id: Enter redshift.
	 * Conn Type: Enter Postgres.
	 * Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
	 * Schema: Enter dev. This is the Redshift database you want to connect to.
	 * Login: Enter awsuser.
	 * Password: Enter the password you created when launching your Redshift cluster.
	 * Port: Enter 5439.

* Connection (for ssh_conn_dataeng_spark)
	* Conn_id: spark_conn_dataeng_spark
	* Host: yarn
	* Login: hadoop
	* extra: {"queue": null, "deploy-mode": null, "spark-binary": "spark-submit", "namespace": "default"}

* Connection (spark_conn_dataeng_spark)
	* Conn Id: ssh_conn_dataeng_spark
	* Conn Type: SSH
	* Host : your EMR cluster's host
	* Port: 22
	* Extra: {"key_file": "/emr-key.pem"}



# My approach the problem differently under the following scenarios:
 * If the data was increased by 100x.
 In this case I might add another node to my cluster.
 * If the pipelines were run on a daily basis by 7am.
 Now my pipeline starts at 24:00 on a daily basis. I could easily change it to 7:00 from dag.
* If the database needed to be accessed by 100+ people.

# Explanation about the technologies choosen:
Since I have big amount of data I needed to process it in a distributed system. 
I choosed Spark, because it lets you run programs up to 100x faster in memory, or 10x faster on disk, than Hadoop's Mapreduce.
I choosed to run  my spark code on a EMR cluster with one master and 2 slave nodes. Because EMR launches clusters fast, it's affordable and reliable. 
I choosed to store my data in Redshift. Becuase it is built for querying big data(a distributed and columnar database). I also stored my data in S3. Thus, even if I terminate my Redshift cluster when I don't needed it, I wouldn't lose my data. 