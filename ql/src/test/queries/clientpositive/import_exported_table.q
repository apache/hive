dfs ${system:test.dfs.mkdir} hdfs:///tmp/test/;

dfs -copyFromLocal ../../data/files/exported_table hdfs:///tmp/test/;

IMPORT FROM '/tmp/test/exported_table';
DESCRIBE j1_41;
SELECT * from j1_41;

dfs -rmr hdfs:///tmp/test;

