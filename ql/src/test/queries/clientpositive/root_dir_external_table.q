dfs ${system:test.dfs.mkdir} hdfs:///tmp/test;

insert overwrite directory "hdfs:///tmp/test" select key from src where (key < 20) order by key;

dfs -cp /tmp/test/000000_0 /000000_0;
dfs -rmr hdfs:///tmp/test;

create external table roottable (key string) row format delimited fields terminated by '\\t' stored as textfile location 'hdfs:///';
select count(*) from roottable;

dfs -rmr /000000_0;