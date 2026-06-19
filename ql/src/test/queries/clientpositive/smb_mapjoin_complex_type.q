set hive.strict.checks.cartesian.product=false;
set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.join.emit.interval=2;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

CREATE TABLE test_list1 (key INT, value array<int>, col_1 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_list1 VALUES (99, array(0,0), 'Alice'), (99, array(2,2), 'Mat'), (100, array(0,0), 'Bob'), (101, array(2,2), 'Car'), (102, array(1, 2, 3, 4), 'Mallory');

CREATE TABLE test_list2 (key INT, value array<int>, col_2 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_list2 VALUES (102, array(2,2), 'Del'), (103, array(2,2), 'Ema'), (104, array(3,3), 'Fli'), (105, array(1, 2, 3, 4), 'Victor');

EXPLAIN
SELECT *
FROM test_list1 INNER JOIN test_list2
ON (test_list1.value=test_list2.value);

SELECT *
FROM test_list1 INNER JOIN test_list2
ON (test_list1.value=test_list2.value);

CREATE TABLE test_struct1 (key INT, value struct<f1: int,f2: string>, col_1 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_struct1 VALUES (99, named_struct("f1", 1, "f2", 'val_0'), 'Alice'),
 (99, named_struct("f1", 2, "f2", 'val_2'), 'Mat'),
 (100, named_struct("f1", 0, "f2", 'val_0'), 'Bob'), (101, named_struct("f1", 2, "f2", 'val_2'), 'Car');

CREATE TABLE test_struct2 (key INT, value struct<f1: int,f2: string>, col_2 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_struct2 VALUES (102, named_struct("f1", 2, "f2", 'val_2'), 'Del'), (103, named_struct("f1", 2, "f2", 'val_2'), 'Ema'),
 (104, named_struct("f1", 3, "f2", 'val_3'), 'Fli');

EXPLAIN
SELECT *
FROM test_struct1  INNER JOIN test_struct2
ON (test_struct1.value=test_struct2.value);

SELECT *
FROM test_struct1  INNER JOIN test_struct2
ON (test_struct1.value=test_struct2.value);
