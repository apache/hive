-- Check we can create a table located in a blobstore
-- with CTAS from a table in HDFS

DROP TABLE IF EXISTS hdfs_source;
CREATE TABLE hdfs_source(a string, b string, c double)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' 
INTO TABLE hdfs_source;

DROP TABLE IF EXISTS blobstore_target;
CREATE TABLE blobstore_target 
LOCATION '${hiveconf:test.blobstore.path.unique}/ctas_hdfs_to_blobstore/blobstore_target/'
AS SELECT * FROM hdfs_source;

DROP DATABASE IF EXISTS target_db;
CREATE DATABASE target_db 
LOCATION '${hiveconf:test.blobstore.path.unique}/ctas_hdfs_to_blobstore/target_db';
CREATE TABLE target_db.blobstore_target
AS SELECT * FROM hdfs_source;

SELECT * FROM hdfs_source;
SELECT * FROM blobstore_target;
SELECT * FROM target_db.blobstore_target;