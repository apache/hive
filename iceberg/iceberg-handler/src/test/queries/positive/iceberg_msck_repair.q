-- SORT_QUERY_RESULTS

create external table ice_msck_repair_test (id int, name string) stored by iceberg stored as orc;

-- First insert - creates some data files
insert into ice_msck_repair_test values (1, 'one'), (2, 'two');

-- List files in the data directory
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/ice_msck_repair_test/data/;

-- Manually delete ALL data files from the first insert to simulate accidental deletion
dfs -rm ${hiveconf:hive.metastore.warehouse.dir}/ice_msck_repair_test/data/*.orc;

-- Now insert more data - this creates new files, but metadata still references the deleted ones
insert into ice_msck_repair_test values (3, 'three'), (4, 'four');
insert into ice_msck_repair_test values (5, 'five'), (6, 'six');

-- List files in the data directory
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/ice_msck_repair_test/data/;

-- Run MSCK REPAIR to remove dangling file references for the deleted files
MSCK REPAIR TABLE ice_msck_repair_test;

-- Verify the table works correctly after repair
select * from ice_msck_repair_test order by id;

drop table ice_msck_repair_test;
