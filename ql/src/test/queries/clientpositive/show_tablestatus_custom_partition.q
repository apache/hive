dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_table/part=1;
dfs ${system:test.dfs.mkdir} pfile://${system:test.tmp.dir}/test_table/part=2;
dfs -copyFromLocal ../../data/files/in1.txt hdfs:///tmp/test_table/part=1/;
dfs -copyFromLocal ../../data/files/in2.txt pfile://${system:test.tmp.dir}/test_table/part=2/;

CREATE EXTERNAL TABLE test_table (col int)
PARTITIONED BY (part int)
LOCATION 'hdfs:///tmp/test_table';

MSCK REPAIR TABLE test_table;

SHOW TABLE EXTENDED LIKE `test_table` PARTITION(part=1);

ALTER TABLE test_table ADD PARTITION(part=2) location 'pfile://${system:test.tmp.dir}/test_table/part=2';

SHOW TABLE EXTENDED LIKE `test_table` PARTITION(part=2);

DROP TABLE test_table;

dfs -rmr hdfs:///tmp/test_table;
dfs -rmr pfile://${system:test.tmp.dir}/test_table;
