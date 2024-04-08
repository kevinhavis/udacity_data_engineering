# Automating Table Creation for the Sparkify Database Using Airflow

## Sparkify
Sparkify is a music streaming service that differentiates itself by using data to improve the user’s experience.


## The Purpose of this Database
The files included in this package are used to create a database of:
- Artist Information
- Song Information
- User Information
- Songplay Data
- Time data

This database takes `json`  files of song data and log data to create tables that can be used for further analysis of listening patterns by users.

This package uses Apache Airflow to automate the table creation process daily using the operators:
- Stage Operator
- Fact and Dimension Operator
- Data Quality Operator

## How This Program is Set Up
This script is set up to extract JSON data from an S3 source, transform the data into tables formatted for business usage using Spark, and load the data back into S3.

The operators are used in order as shown in the below diagram:
<img src='example-dag.png' title='Working DAG for Sparkify'>

### stage_redshift.py
This operator copies data from the source S3 location to the designated Redshift database.

### load_fact.py and load_dimension.py
These operators will take the staging data and load it into fact and dimension tables for analysis.

### data_quality.py
This operator checks to make sure that data has been loaded into the target tables.

## Database Schema Design
The database schema consists of the dimension tables:
- **artists**
	- *artist\_id, name, location, latitude, longitude* 
- **songs**
	- *song\_id, title, artist\_id, year, duration* 
- **users**
	- *user\_id, first\_name, last\_name, gender, level* 
- **time**
	- *start\_time, hour, day, week, month, year, weekday*

and the fact table:
- **songplays**
	- *songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent* 

This database is based on a star schema. A star schema is well formatted for deriving business insights and is simple to understand, which makes it ideal for this initial endeavor to quickly derive songplay patterns from a database. As new stakeholder needs are identified the option to move to a different schema will be evaluated.