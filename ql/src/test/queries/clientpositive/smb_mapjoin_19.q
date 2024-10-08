--! qt:dataset:src
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;


set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

set hive.metastore.aggregate.stats.cache.enabled=false;
set hive.cbo.fallback.strategy=NEVER;

-- Create two bucketed and sorted tables
CREATE TABLE test_table1_n11 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 16 BUCKETS;
CREATE TABLE test_table2_n11 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 16 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n11 PARTITION (ds = '1') SELECT *;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN
INSERT OVERWRITE TABLE test_table2_n11 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1_n11 a WHERE a.ds = '1';

INSERT OVERWRITE TABLE test_table2_n11 PARTITION (ds = '1')
SELECT a.key, a.value FROM test_table1_n11 a WHERE a.ds = '1';

select count(*) from test_table1_n11 where ds = '1';
select count(*) from test_table1_n11 where ds = '1' and hash(key) % 16 = 0;
select count(*) from test_table1_n11 where ds = '1' and hash(key) % 16 = 5;
select count(*) from test_table1_n11 where ds = '1' and hash(key) % 16 = 12;
select count(*) from test_table1_n11 tablesample (bucket 1 out of 16) s where ds = '1';
select count(*) from test_table1_n11 tablesample (bucket 6 out of 16) s where ds = '1';
select count(*) from test_table1_n11 tablesample (bucket 13 out of 16) s where ds = '1';

select count(*) from test_table2_n11 where ds = '1';
select count(*) from test_table2_n11 where ds = '1' and hash(key) % 16 = 0;
select count(*) from test_table2_n11 where ds = '1' and hash(key) % 16 = 5;
select count(*) from test_table2_n11 where ds = '1' and hash(key) % 16 = 12;
select count(*) from test_table2_n11 tablesample (bucket 1 out of 16) s where ds = '1';
select count(*) from test_table2_n11 tablesample (bucket 6 out of 16) s where ds = '1';
select count(*) from test_table2_n11 tablesample (bucket 13 out of 16) s where ds = '1';
