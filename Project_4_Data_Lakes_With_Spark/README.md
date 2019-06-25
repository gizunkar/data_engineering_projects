# Data Lakes with Spark 

The company Sparkify provides a music streaming app to their users. The analytics team wants to understand what songs users are listening to by analyzing their songs and user activity data. 

They keep their data currently in S3 buckets. The goal of this project is to buit an ETL pipeline that extracts their data from S3, processes them using Spark on Amazon EMR and loads the data back into S3 in parquet format.

## Data Model

We have two source datasets for this project, which are both stored in JSON format. 
 * Song Dataset: It contains metadata about songs and artists of songs.
 * Log Dataset: It contains logs on user activity on the app.

ER diagram of data model is below.


![Image of ER Diagram](https://raw.githubusercontent.com/gizunkar/git_tutorial/master/aws_etl_er_diagram.png)


## Project Files  

1. **dl.cfg** : It's a configuration file which contains your IAM user and Access Key. 

2. **etl.py** : It processes data using Spark and loads it to S3 bucket.



## How to Run 


  ``` console
  import etl
  etl.main()
```



