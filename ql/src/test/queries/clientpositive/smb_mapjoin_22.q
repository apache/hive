--! qt:dataset:src
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;


set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- Create two bucketed and sorted tables
CREATE TABLE test_table1_n10 (key INT, value STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2_n10 (key INT, value STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n10 SELECT *;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN INSERT OVERWRITE TABLE test_table2_n10
SELECT * FROM test_table1_n10;

INSERT OVERWRITE TABLE test_table2_n10
SELECT * FROM test_table1_n10;

select count(*) from test_table1_n10;
select count(*) from test_table1_n10 tablesample (bucket 2 out of 2) s;

select count(*) from test_table2_n10;
select count(*) from test_table2_n10 tablesample (bucket 2 out of 2) s;

drop table test_table1_n10;
drop table test_table2_n10;

CREATE TABLE test_table1_n10 (key INT, value STRING)
CLUSTERED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2_n10 (key INT, value STRING)
CLUSTERED BY (key) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n10 SELECT *;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN INSERT OVERWRITE TABLE test_table2_n10
SELECT * FROM test_table1_n10;

INSERT OVERWRITE TABLE test_table2_n10
SELECT * FROM test_table1_n10;

select count(*) from test_table1_n10;
select count(*) from test_table1_n10 tablesample (bucket 2 out of 2) s;

select count(*) from test_table2_n10;
select count(*) from test_table2_n10 tablesample (bucket 2 out of 2) s;
