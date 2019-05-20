SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS test1_n9;
DROP TABLE IF EXISTS test2_n6;

CREATE TABLE test1_n9(name string, age int);
CREATE TABLE test2_n6(name string) PARTITIONED by (age int);

LOAD DATA LOCAL INPATH '../../data/files/test1.txt' INTO TABLE test1_n9;
FROM test1_n9 INSERT OVERWRITE TABLE test2_n6 PARTITION(age) SELECT test1_n9.name, test1_n9.age;

ANALYZE TABLE test2_n6 PARTITION(age) COMPUTE STATISTICS;

-- To show stats. It doesn't show due to a bug.
DESC EXTENDED test2_n6;

-- Another way to show stats.
EXPLAIN EXTENDED select * from test2_n6;

DROP TABLE test1_n9;
DROP TABLE test2_n6;
