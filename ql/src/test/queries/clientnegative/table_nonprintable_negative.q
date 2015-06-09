set hive.msck.path.validation=throw;

dfs ${system:test.dfs.mkdir} hdfs:///tmp/temp_table_external/day=Foo;
dfs -copyFromLocal ../../data/files/in1.txt hdfs:///tmp/temp_table_external/day=Foo;
dfs -ls hdfs:///tmp/temp_table_external/day=Foo;

create external table table_external (c1 int, c2 int)
partitioned by (day string)
location 'hdfs:///tmp/temp_table_external';

msck repair table table_external;
