--! qt:dataset:src
set hive.mapred.mode=nonstrict;
dfs ${system:test.dfs.mkdir} file:///tmp/test_sa1;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_sa1;

create external table dynPart (key string) partitioned by (value string) row format delimited fields terminated by '\\t' stored as textfile;
insert overwrite local directory "/tmp/test_sa1" select key from src where (key = 10) order by key;
insert overwrite directory "/tmp/test_sa1" select key from src where (key = 20) order by key;
alter table dynPart add partition (value='0') location 'file:///tmp/test_sa1';
alter table dynPart add partition (value='1') location 'hdfs:///tmp/test_sa1';
select count(*) from dynPart;
select key from dynPart;
select key from src where (key = 10) order by key;
select key from src where (key = 20) order by key;

dfs -rmr file:///tmp/test_sa1;
dfs -rmr hdfs:///tmp/test_sa1;
