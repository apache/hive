-- Test simple interaction with partitioned bucketed table with paquet format in blobstore

SET hive.exec.dynamic.partition=true;
SET hive.exec.reducers.max=10;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE blobstore_source;
CREATE TABLE blobstore_source(a STRING, b STRING, c DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_buckets/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE blobstore_source;

DROP TABLE parquet_buckets;
CREATE TABLE parquet_buckets (a STRING, value DOUBLE)
PARTITIONED BY (b STRING)
CLUSTERED BY (a) INTO 10 BUCKETS
STORED AS PARQUET
LOCATION '${hiveconf:test.blobstore.path.unique}/parquet_buckets/parquet_buckets';

INSERT OVERWRITE TABLE parquet_buckets
PARTITION (b)
SELECT a, c, b FROM blobstore_source;
SELECT * FROM parquet_buckets;

INSERT INTO TABLE parquet_buckets
PARTITION (b)
SELECT a, c, b FROM blobstore_source;
SELECT * FROM parquet_buckets;