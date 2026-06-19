-- Test simple interaction with partitioned bucketed table with rcfile format in blobstore

SET hive.exec.dynamic.partition=true;
SET hive.exec.reducers.max=10;

DROP TABLE blobstore_source;
CREATE TABLE blobstore_source(a STRING, b STRING, c DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/rcfile_buckets/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE blobstore_source;

DROP TABLE rcfile_buckets;
CREATE TABLE rcfile_buckets (a STRING, value DOUBLE)
PARTITIONED BY (b STRING)
CLUSTERED BY (a) INTO 10 BUCKETS
STORED AS RCFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/rcfile_buckets/rcfile_buckets';

INSERT OVERWRITE TABLE rcfile_buckets
PARTITION (b)
SELECT a, c, b FROM blobstore_source;
SELECT * FROM rcfile_buckets;

INSERT INTO TABLE rcfile_buckets
PARTITION (b)
SELECT a, c, b FROM blobstore_source;
SELECT * FROM rcfile_buckets;