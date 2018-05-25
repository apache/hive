set hive.mapred.mode=nonstrict;
set hive.exec.infer.bucket.sort=true;
;


-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata, in particular, this tests
-- that operators in the mapper have no effect

CREATE TABLE test_table1_n14 (key STRING, value STRING)
CLUSTERED BY (key) SORTED BY (key DESC) INTO 2 BUCKETS;

CREATE TABLE test_table2_n13 (key STRING, value STRING)
CLUSTERED BY (key) SORTED BY (key DESC) INTO 2 BUCKETS;

INSERT OVERWRITE TABLE test_table1_n14 SELECT key, value FROM src;

INSERT OVERWRITE TABLE test_table2_n13 SELECT key, value FROM src;

CREATE TABLE test_table_out_n0 (key STRING, value STRING) PARTITIONED BY (part STRING);

set hive.map.groupby.sorted=true;

-- Test map group by doesn't affect inference, should not be bucketed or sorted
EXPLAIN INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1') 
SELECT key, count(*) FROM test_table1_n14 GROUP BY key;

INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1') 
SELECT key, count(*) FROM test_table1_n14 GROUP BY key;

DESCRIBE FORMATTED test_table_out_n0 PARTITION (part = '1');

-- Test map group by doesn't affect inference, should be bucketed and sorted by value
EXPLAIN INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1') 
SELECT a.key, a.value FROM (
	SELECT key, count(*) AS value FROM test_table1_n14 GROUP BY key
) a JOIN (
 	SELECT key, value FROM src
) b
ON (a.value = b.value);

INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1') 
SELECT a.key, a.value FROM (
	SELECT key, cast(count(*) AS STRING) AS value FROM test_table1_n14 GROUP BY key
) a JOIN (
 	SELECT key, value FROM src
) b
ON (a.value = b.value);

DESCRIBE FORMATTED test_table_out_n0 PARTITION (part = '1');

set hive.map.groupby.sorted=false;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.cbo.enable=false;

-- Test SMB join doesn't affect inference, should not be bucketed or sorted
EXPLAIN INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ a.key, b.value FROM test_table1_n14 a JOIN test_table2_n13 b ON a.key = b.key;

INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ a.key, b.value FROM test_table1_n14 a JOIN test_table2_n13 b ON a.key = b.key;

DESCRIBE FORMATTED test_table_out_n0 PARTITION (part = '1');

-- Test SMB join doesn't affect inference, should be bucketed and sorted by key
EXPLAIN INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ b.value, count(*) FROM test_table1_n14 a JOIN test_table2_n13 b ON a.key = b.key
GROUP BY b.value;

INSERT OVERWRITE TABLE test_table_out_n0 PARTITION (part = '1')
SELECT /*+ MAPJOIN(a) */ b.value, count(*) FROM test_table1_n14 a JOIN test_table2_n13 b ON a.key = b.key
GROUP BY b.value;

DESCRIBE FORMATTED test_table_out_n0 PARTITION (part = '1');

