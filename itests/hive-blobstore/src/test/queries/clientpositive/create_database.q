-- Test tables with location inherited from database

CREATE DATABASE db_with_location LOCATION '${hiveconf:test.blobstore.path.unique}/create_database/db_with_location/';
USE db_with_location;
SHOW TABLES;

DROP TABLE events;
DROP TABLE test_table;

CREATE TABLE test_table (col1 STRING) STORED AS TEXTFILE;
SHOW TABLES;

CREATE EXTERNAL TABLE events(
    userUid STRING,
    trackingId STRING,
    eventType STRING,
    action STRING,
    url STRING)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ';

dfs -mkdir -p ${hiveconf:test.blobstore.path.unique}/create_database/db_with_location/events/dt=2010-12-08;
dfs -copyFromLocal ../../data/files/5col_data.txt ${hiveconf:test.blobstore.path.unique}/create_database/db_with_location/events/dt=2010-12-08/;

MSCK REPAIR TABLE events;

SHOW TABLES;

SHOW PARTITIONS events;

DROP TABLE events;

USE DEFAULT;

SHOW TABLES from db_with_location;

USE db_with_location;
DROP TABLE test_table;

DROP database db_with_location;
USE DEFAULT;
