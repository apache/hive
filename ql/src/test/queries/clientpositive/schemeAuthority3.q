--! qt:dataset:src
set hive.mapred.mode=nonstrict;

create table noschemeTable(key string) partitioned by (value string, value2 string) row format delimited fields terminated by '\\t' stored as textfile;
insert into noschemeTable partition(value='0', value2='clusterA') select key from src where (key = 10) order by key;

alter table noschemeTable set location '/tmp/newtest';
alter table noschemeTable partition (value='0', value2='clusterA') set location '/tmp/newtest2/value=0/value2=clusterA';
