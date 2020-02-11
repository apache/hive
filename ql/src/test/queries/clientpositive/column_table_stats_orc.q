set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS s_n0;

CREATE TABLE s_n0 (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS ORC;

insert into table s_n0 values ('1','2');

desc formatted s_n0;

explain extended analyze table s_n0 compute statistics for columns;

analyze table s_n0 compute statistics for columns;

desc formatted s_n0;

DROP TABLE IF EXISTS spart_n0;

CREATE TABLE spart_n0 (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS ORC;

insert into table spart_n0 PARTITION (ds="2008-04-08", hr="12") values ('1','2');
insert into table spart_n0 PARTITION (ds="2008-04-08", hr="11") values ('1','2');

desc formatted spart_n0;

explain extended analyze table spart_n0 compute statistics for columns;

analyze table spart_n0 compute statistics for columns;

desc formatted spart_n0;

desc formatted spart_n0 PARTITION(ds='2008-04-08', hr=11);
desc formatted spart_n0 PARTITION(ds='2008-04-08', hr=12);


DROP TABLE IF EXISTS spart_n0;

CREATE TABLE spart_n0 (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS ORC;

insert into table spart_n0 PARTITION (ds="2008-04-08", hr="12") values ('1','2');
insert into table spart_n0 PARTITION (ds="2008-04-08", hr="11") values ('1','2');

desc formatted spart_n0;

explain extended analyze table spart_n0 partition(hr="11") compute statistics for columns;

analyze table spart_n0 partition(hr="11") compute statistics for columns;

desc formatted spart_n0;

desc formatted spart_n0 PARTITION(ds='2008-04-08', hr=11);
desc formatted spart_n0 PARTITION(ds='2008-04-08', hr=12);
