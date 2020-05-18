--! qt:disabled:disabled by 54b4b2d428bb in 2018
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.llap.io.enabled=false;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_root_dir_external_table;

insert overwrite directory "hdfs:///tmp/test_root_dir_external_table" select key from src where (key < 20) order by key;

dfs -cp /tmp/test_root_dir_external_table/000000_0 /000000_0;
dfs -rmr hdfs:///tmp/test_root_dir_external_table;

create external table roottable_n0 (key string) row format delimited fields terminated by '\\t' stored as textfile location 'hdfs:///';
select count(*) from roottable_n0;

dfs -rmr /000000_0;
