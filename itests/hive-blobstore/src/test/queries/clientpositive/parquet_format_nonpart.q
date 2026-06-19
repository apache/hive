-- Test INSERT OVERWRITE and INSERT INTO on parquet table in blobstore

DROP TABLE blobstore_source;
CREATE TABLE blobstore_source(a STRING, b STRING, c DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_format_nonpart/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE blobstore_source;

DROP TABLE parquet_table;
CREATE EXTERNAL TABLE parquet_table (a INT, b STRING, value DOUBLE) STORED AS PARQUET
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_format_nonpart/parquet_table';
 
INSERT OVERWRITE TABLE parquet_table
SELECT * FROM blobstore_source;
 
SELECT * FROM parquet_table;
SELECT a FROM parquet_table GROUP BY a;
SELECT b FROM parquet_table GROUP BY b;
SELECT value FROM parquet_table GROUP BY value;

INSERT INTO TABLE parquet_table
SELECT * FROM blobstore_source;

SELECT * FROM parquet_table;
SELECT a FROM parquet_table GROUP BY a;
SELECT b FROM parquet_table GROUP BY b;
SELECT value FROM parquet_table GROUP BY value;