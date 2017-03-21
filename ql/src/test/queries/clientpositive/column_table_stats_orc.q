set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS s;

CREATE TABLE s (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS ORC;

insert into table s values ('1','2');

desc formatted s;

explain extended analyze table s compute statistics for columns;

analyze table s compute statistics for columns;

desc formatted s;

DROP TABLE IF EXISTS spart;

CREATE TABLE spart (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS ORC;

insert into table spart PARTITION (ds="2008-04-08", hr="12") values ('1','2');
insert into table spart PARTITION (ds="2008-04-08", hr="11") values ('1','2');

desc formatted spart;

explain extended analyze table spart compute statistics for columns;

analyze table spart compute statistics for columns;

desc formatted spart;

desc formatted spart PARTITION(ds='2008-04-08', hr=11);
desc formatted spart PARTITION(ds='2008-04-08', hr=12);


DROP TABLE IF EXISTS spart;

CREATE TABLE spart (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS ORC;

insert into table spart PARTITION (ds="2008-04-08", hr="12") values ('1','2');
insert into table spart PARTITION (ds="2008-04-08", hr="11") values ('1','2');

desc formatted spart;

explain extended analyze table spart partition(hr="11") compute statistics for columns;

analyze table spart partition(hr="11") compute statistics for columns;

desc formatted spart;

desc formatted spart PARTITION(ds='2008-04-08', hr=11);
desc formatted spart PARTITION(ds='2008-04-08', hr=12);
