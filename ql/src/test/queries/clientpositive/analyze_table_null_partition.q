SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;

CREATE TABLE test1(name string, age int);
CREATE TABLE test2(name string) PARTITIONED by (age int);

LOAD DATA LOCAL INPATH '../../data/files/test1.txt' INTO TABLE test1;
FROM test1 INSERT OVERWRITE TABLE test2 PARTITION(age) SELECT test1.name, test1.age;

ANALYZE TABLE test2 PARTITION(age) COMPUTE STATISTICS;

-- To show stats. It doesn't show due to a bug.
DESC EXTENDED test2;

-- Another way to show stats.
EXPLAIN EXTENDED select * from test2;

DROP TABLE test1;
DROP TABLE test2;
