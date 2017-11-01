-- Test bucketed table

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS source;
CREATE TABLE source (a STRING, b STRING, c DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/buckets/source';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE source;

DROP TABLE IF EXISTS buckets;
CREATE TABLE buckets (a STRING, value DOUBLE)
PARTITIONED BY (b STRING)
CLUSTERED BY (a) INTO 2 BUCKETS
LOCATION '${hiveconf:test.blobstore.path.unique}/buckets/buckets';

INSERT OVERWRITE TABLE buckets
PARTITION (b)
SELECT a, c, b FROM source;

SELECT * FROM buckets;

INSERT INTO TABLE buckets
PARTITION (b)
SELECT a, c, b FROM source;

SELECT * FROM buckets;
