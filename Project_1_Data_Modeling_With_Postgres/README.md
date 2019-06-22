# Data Modeling with Postgres


The company Sparkify provides a music streaming app to their users. The analytics team wants to understand what songs users are listening to by analyzing their songs and user activity data. 

The goal of this project is to create a database schema in Postgres and ETL pipeline for this analysis. 

# Data Model

There're two source datasets in this project. One is located in a directory of JSON logs on user activity on the app and the second is in a directory with JSON metadata on the songs in their app. This data structure isn't useful for analytic use cases.

For analytic purposes, a reporting schema (OLAP) should be designed and implemented. Because these type of schemas are optimized for bulk reads and aggregations. 

That's why, a star schema is designed and implemented, which consists of one fact table and multiple dimension tables. 

* Fact table: It contains facts (or measures) of a business process and foreign keys to dimensions. 

  * **songplays**: This table contains records in log dataset associated with song plays that page attribute is equal to "NextSong".

* Dimension tables: It contains descriptive characteristics of the facts.

  * **users**: It's the dimension of users in the app. 
  * **songs**: It's the dimension of songs in music database.
  * **artists**: It's the dimension of artists in music database.
  * **time**: This table contains timestamps in songplays table which are broken down into some units.


ER Diagram of data model is below:

![Image of ER Diagram](https://r766469c826263xjupyterllyjhwqkl.udacity-student-workspaces.com/files/ER_diagram.png)


### Mapping

  * **users table mapping**:  
    * **source**: It's user activity log data which is located in path *"data/log_data"*.
    * **filter conditions:** Log data should be filtered by field *page*='NextSong'.

|Target column | Target data type | Source field | Mapping |
| :---         |   :----:         |    :----:    | ---:|
| user_id      | int              | userId       |as is|
| first_name   | varchar     	  | firstName    |as is|
| last_name    | varchar          |lastName      |as is|
| gender       | varchar     	  | gender    	 |as is|
| level        | varchar     	  | level        |as is|


  * **songs table mapping**: 
    * **source**: It's song data which is located in *"data/song_data"*.

| Target column	| Target data type 				| Source field  | Mapping  |
| :---         	|         :----:    			|    :----:     | ---:     |
| song_id      	| varchar not null Primary Key  | song_id       |as is|
| title        	| varchar     	  				| title         |as is|
| artist_id    	| varchar          				| artist_id     |as is|
| year         	| int    	 	  				| year    	   |as is|
| duration     	| float	     	  				| duration      |as is|


  * **artists table mapping**

    * **source**: It's song data which is located in *"data/song_data"*.

| Target column	 |   Target data type 			   |  Source field 	 | Mapping  |
| :---           |     :----:         			   |    :----:     	 | ---:|
| artist_id      |   varchar NOT NULL Primary Key  | artist_id     	 |as is|
| name      	 |   varchar     	  			   | artist_name     |as is|
| location  	 |   varchar        			   | artist_location   |as is|
| latitude  	 |   numeric     	  			   | artist_lattitude  |as is|
| longitude      |   numeric     	  			   | artist_longitude  |as is|
				

  * **time table mapping** :    
  	* **source**: It's the user activity log data which is located in *"data/log_data"*.
  	* **filter condition:** Log data should be filtered by field *page*='NextSong'.


|Target column|  Target data type 				| Source field 	 | Mapping  |
|   :---      |    :----:         				|       :----:   | ---:|
|start_time   |  timestamp NOT NULL Primary Key | ts             |as is|
|hour    	  |  int     	  	  				| ts             |extract hour of ts|
|day      	  |  int          	  				| ts             |extract day of ts|
|week         |  int     	      				| ts             |extract week of ts|
|month     	  |  int     	      				| ts             |extract month of ts|
|year     	  |  int     	      				| ts             |extract year of ts|
|weekday      |  varchar     	  				| ts             |extract the day of week of ts|

* **songplays table mapping**: 
  * **sources**: 
    1. user activity log data (located in path data/log_data).
    2. artist table 
    3. users table in Postgres. 
  * **filter condition:** Log data should be filtered by field *page*='NextSong'.
  * **join condition:** *artists* and *users* tables must be joined on artist_id columns and matching records based on song_title, artist_name, and duration should be found to find song_id and artist_id.



| Target column | Target data type   |Source 		| Source field 	| Mapping  |
|    :----:     |   :----:           | :---  		| :----:        | ---:|
| songplay_id   | serial Primary Key | NA			| NA       		|auto incremental id |
| start_time    | timestamp      	 | log dataset	| NA            |as is|
| user_id       | int      	  	  	 | users			| NA        	|as is|
| level         | varchar      	  	 | log dataset	| NA            |as is|
| song_id       | varchar          	 | log dataset	| NA            |as is|
| artist_id     | varchar     	     | artists		| NA        	|as is|
| session_id    | varchar     	  	 | log dataset	| NA            |as is|
| location     	| varchar     	     | log dataset	| NA            |as is|
| user_agent    | varchar     	  	 | log dataset	| NA            |as is|


## Package Dependencies

*psycopg2* is a popular PostgreSQL database adapter for the Python programming language. It should be installed by running the following command.

``` console
pip install psycopg2
```

*pandas* must be installed.

``` console
pip install pandas
```

## Project Files  

1. **sql_queries.py**   : It contains all the sql queries. It should be imported in following python files.

2. **create_tables.py** : It contains functions that drops and creates Sparkify database and all tables in ETL pipeline. 
It should be run first.

  * **Module Dependencies**

    * **external dependency:** psycopg2
    * **internal dependency:** sql_queries

  * **Functions**

    * create_database() : This function drops and creates sparkify database.
    * drop_tables(cur, conn): This function drops existing tables.
    * create_tables(cur, conn): This function creates tables. 
    * main(): This function calls *create_database*, *drop_tables*, *create_tables* functions respectively.

3. **etl.py** : It reads and processes files from data/log_data and data/song_data and loads them into DB tables. It should be run after *create_tables.py*.

  * **Module Dependencies**
    * **external dependency:** 
    os,
    glob,
    psycopg2,
    pandas

    * **internal dependency:** sql_queries

  * **Functions**

    * process_song_file(cur, filepath): It reads a song file and inserts records into artists and songs tables. 
    * process_log_file(cur, filepath): It reads a log file, filters dataframe based on business rule and inserts records into users, time and songplays tables. 
    * process_data(cur, conn, filepath, func): It gets files in given directory and applies given function. It is used in main() function to read all log and song files and apply *process_log_file* and *process_song_file* functions. 
    * main(): It calls functions *process_data* for log data and song data.

## How to Run 

Files should be run in the following order. 

  ``` console
  import create_tables
  create_tables.main()
  import etl
  etl.main()
```



  