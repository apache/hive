set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS s;

CREATE TABLE s (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE s;

desc formatted s;

explain extended analyze table s compute statistics for columns;

analyze table s compute statistics for columns;

desc formatted s;

DROP TABLE IF EXISTS spart;

CREATE TABLE spart (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt"
OVERWRITE INTO TABLE spart PARTITION (ds="2008-04-08", hr="11");

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt"
OVERWRITE INTO TABLE spart PARTITION (ds="2008-04-08", hr="12");


desc formatted spart;

explain extended analyze table spart compute statistics for columns;

analyze table spart compute statistics for columns;

desc formatted spart;

desc formatted spart PARTITION(ds='2008-04-08', hr=11);
desc formatted spart PARTITION(ds='2008-04-08', hr=12);

DROP TABLE IF EXISTS spart;

CREATE TABLE spart (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt"
OVERWRITE INTO TABLE spart PARTITION (ds="2008-04-08", hr="11");

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt"
OVERWRITE INTO TABLE spart PARTITION (ds="2008-04-08", hr="12");


desc formatted spart;

explain extended analyze table spart partition(ds,hr) compute statistics for columns;

analyze table spart partition(ds,hr) compute statistics for columns;

desc formatted spart;

desc formatted spart PARTITION(ds='2008-04-08', hr=11);
desc formatted spart PARTITION(ds='2008-04-08', hr=12);

DROP TABLE IF EXISTS spart;

CREATE TABLE spart (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt"
OVERWRITE INTO TABLE spart PARTITION (ds="2008-04-08", hr="11");

LOAD DATA LOCAL INPATH "../../data/files/kv1.txt"
OVERWRITE INTO TABLE spart PARTITION (ds="2008-04-08", hr="12");


desc formatted spart;

explain extended analyze table spart partition(hr="11") compute statistics for columns;

analyze table spart partition(hr="11") compute statistics for columns;

desc formatted spart;

desc formatted spart PARTITION(ds='2008-04-08', hr=11);
desc formatted spart PARTITION(ds='2008-04-08', hr=12);
