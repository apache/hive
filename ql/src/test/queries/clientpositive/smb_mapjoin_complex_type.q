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
INSERT INTO test_list1 VALUES (99, array(0,0), 'Alice'), (99, array(2,2), 'Mat'), (100, array(0,0), 'Bob'), (101, array(2,2), 'Car');

CREATE TABLE test_list2 (key INT, value array<int>, col_2 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_list2 VALUES (102, array(2,2), 'Del'), (103, array(2,2), 'Ema'), (104, array(3,3), 'Fli');

EXPLAIN
SELECT *
FROM test_list1 INNER JOIN test_list2
ON (test_list1.value=test_list2.value);

SELECT *
FROM test_list1 INNER JOIN test_list2
ON (test_list1.value=test_list2.value);


CREATE TABLE test_map1 (key INT, value map<int, int>, col_1 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_map1 VALUES (99, map(0,0), 'Alice'), (99, map(2,2), 'Mat'), (100, map(0,0), 'Bob'), (101, map(2,2), 'Car');

CREATE TABLE test_map2 (key INT, value map<int, int>, col_2 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_map2 VALUES (102, map(2,2), 'Del'), (103, map(2,2), 'Ema'), (104, map(3,3), 'Fli');

EXPLAIN
SELECT *
FROM test_map1  INNER  JOIN test_map2
ON (test_map1.value=test_map2.value);

SELECT *
FROM test_map1  INNER JOIN test_map2
ON (test_map1.value=test_map2.value);


CREATE TABLE test_union1 (key INT, value UNIONTYPE<int, string>, col_1 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_union1 VALUES (99, create_union(0,0,"val_0"), 'Alice'), (99, create_union(0,2, "val_2"), 'Mat'),
 (100, create_union(0,0, "val_0"), 'Bob'), (101, create_union(0,2,"val_2"), 'Car');

CREATE TABLE test_union2 (key INT, value UNIONTYPE<int, string>, col_2 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT INTO test_union2 VALUES (102, create_union(0,2,"val_2"), 'Del'), (103, create_union(0,2,"val_2"), 'Ema'),
 (104, create_union(0,3,"val_3"), 'Fli');

EXPLAIN
SELECT *
FROM test_union1  INNER JOIN test_union2
ON (test_union1.value=test_union2.value);

SELECT *
FROM test_union1  INNER JOIN test_union2
ON (test_union1.value=test_union2.value);