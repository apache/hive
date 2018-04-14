--! qt:dataset:src
set hive.mapred.mode=nonstrict;
dfs ${system:test.dfs.mkdir} file:///tmp/test_sa2;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_sa2;

create external table dynPart_n0 (key string) partitioned by (value string, value2 string) row format delimited fields terminated by '\\t' stored as textfile;
insert overwrite local directory "/tmp/test_sa2" select key from src where (key = 10) order by key;
insert overwrite directory "/tmp/test_sa2" select key from src where (key = 20) order by key;
alter table dynPart_n0 add partition (value='0', value2='clusterA') location 'file:///tmp/test_sa2';
alter table dynPart_n0 add partition (value='0', value2='clusterB') location 'hdfs:///tmp/test_sa2';
select value2, key from dynPart_n0 where value='0';

dfs -rmr file:///tmp/test_sa2;
dfs -rmr hdfs:///tmp/test_sa2;
