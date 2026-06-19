-- Check we can create a table located in HDFS
-- with CTAS from a table a blobstore

DROP TABLE IF EXISTS blobstore_source;
CREATE TABLE blobstore_source(a string, b string, c double)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/ctas_blobstore_to_hdfs/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' 
INTO TABLE blobstore_source;

DROP TABLE IF EXISTS hdfs_target;
CREATE TABLE hdfs_target 
AS SELECT * FROM blobstore_source;

DROP DATABASE IF EXISTS target_db;
CREATE DATABASE target_db;
CREATE TABLE target_db.hdfs_target
AS SELECT * FROM blobstore_source;

SELECT * FROM blobstore_source;
SELECT * FROM hdfs_target;
SELECT * FROM target_db.hdfs_target;