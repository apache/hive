-- Test simple interaction with partitioned bucketed table with orc format in blobstore

SET hive.exec.dynamic.partition=true;
SET hive.exec.reducers.max=10;

DROP TABLE blobstore_source;
CREATE TABLE blobstore_source(a STRING, b STRING, c DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_buckets/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE blobstore_source;

DROP TABLE orc_buckets;
CREATE TABLE orc_buckets (a STRING, value DOUBLE)
PARTITIONED BY (b STRING)
CLUSTERED BY (a) INTO 10 BUCKETS
STORED AS ORC
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_buckets/orc_buckets';

INSERT OVERWRITE TABLE orc_buckets
PARTITION (b)
SELECT a, c, b FROM blobstore_source;
SELECT * FROM orc_buckets;

INSERT INTO TABLE orc_buckets
PARTITION (b)
SELECT a, c, b FROM blobstore_source;
SELECT * FROM orc_buckets;