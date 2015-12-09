set hive.msck.path.validation=skip;
set hive.mapred.mode=nonstrict;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/temp_table_external/day=¢Bar;
dfs -copyFromLocal ../../data/files/in1.txt hdfs:///tmp/temp_table_external/day=¢Bar;
dfs -ls hdfs:///tmp/temp_table_external/day=¢Bar;

dfs ${system:test.dfs.mkdir} hdfs:///tmp/temp_table_external/day=Foo;
dfs -copyFromLocal ../../data/files/in1.txt hdfs:///tmp/temp_table_external/day=Foo;
dfs -ls hdfs:///tmp/temp_table_external/day=Foo;

dfs -ls hdfs:///tmp/temp_table_external;

create external table table_external (c1 int, c2 int)
partitioned by (day string)
location 'hdfs:///tmp/temp_table_external';

msck repair table table_external;

dfs -ls hdfs:///tmp/temp_table_external;

show partitions table_external;
select * from table_external;

alter table table_external drop partition (day='¢Bar');

show partitions table_external;

drop table table_external;

dfs -rmr hdfs:///tmp/temp_table_external;
