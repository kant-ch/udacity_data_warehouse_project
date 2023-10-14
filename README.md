# Project Data Warehouse

## Project overview
In this project, We are requested to build data warehouse for analytical purpose. The data source are located at S3. We need to build ETL pipeline to extract data to redshift. The ETL pipeline consist of the below.

1. Copy data in json format from S3 and storage at staging table in redshift. There are 2 set of data for staging.

    1.1 Event data - Transaction of song that are played by the users
    
    1.2 Song data -  List of song available

2. Transform the raw data (staging table) to fact and dimension table.

    2.1 `songplay` - fact table

    2.2 `users` - dimension table

    2.3 `songs` - dimension table

    2.4 `artists` - dimension table

    2.5 `time` - dimension table
---
## Project Execution
There are 4 files for running the pipeline starting from create table.
1. `dwh.cfg` - Configration file consists of redshift configuration, role, and data source location (S3).
2. `sql_quries.py` - All quries are located in this file. The quries consist of drop table quries (for rerunning without error), create table quries, and insert data quries.
3. `create_tables.py` - Python script for running the drop and create fact and dimension table.
4. `etl.py` - Python script for running the insert data to fact and dimension table.

**Remark** - Due to large amount of data, We can test our pipeline by using small song data set. For testing pipeline, we can use `etl_small_data.py` instead of `etl.py` for reducing runtime.

After we completed all script for ETL pipeline, we used the terminal for running the etl process following by the step below.
1. Run `create_tables.py` for creating all fact and dimension tables 

```
python create_table.py
```
2. Run `etl.py` for copy the data to staging table, then insert data from staging table to fact and dimension tables
```
python etl.py
```