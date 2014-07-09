
dfs ${system:test.dfs.mkdir} hdfs:///tmp/temp_table_external;
dfs -copyFromLocal ../../data/files/in1.txt hdfs:///tmp/temp_table_external/;
dfs -ls hdfs:///tmp/temp_table_external/;

create temporary external table temp_table_external (c1 int, c2 int) location 'hdfs:///tmp/temp_table_external';
select * from temp_table_external;

-- Even after we drop the table, the data directory should still be there
drop table temp_table_external;
dfs -ls hdfs:///tmp/temp_table_external/;

dfs -rmr hdfs:///tmp/temp_table_external;
