-- HIVE-29490: Test hive.use.scratchdir.for.staging configuration and
-- dynamic partition staging directory layout change for native non-acid,
-- non-mm, non-direct-insert external tables.

-- SORT_QUERY_RESULTS

--! qt:replace:/(COLUMN_STATS_ACCURATE\s+)\{.*/$1#Masked#/
--! qt:replace:/(transient_lastDdlTime\s+)[0-9]+/$1#Masked#/

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.autogather=true;
set hive.exec.max.dynamic.partitions=500;
set hive.exec.max.dynamic.partitions.pernode=500;

create database mydb;
create external table mydb.source_test_tbl (a string) stored as orc;
create external table mydb.target_test_tbl (a string) partitioned by (created string, day string) stored as orc;

insert into table mydb.source_test_tbl values ('a');
insert into table mydb.source_test_tbl values ('b');

-- Staging dir sits above the static partition in the table directory:
-- <table_path>/<staging_dir>/<static_partition>/<dynamic_partition>

set hive.use.scratchdir.for.staging=false;
set hive.exec.stagingdir=.hive-staging;
insert into table mydb.target_test_tbl partition (created='20250101', day)
select a, '20250101' as day
from mydb.source_test_tbl;

-- Verify data is correctly written to the final partition.
-- If staging were broken the partition would be empty or missing.
select * from mydb.target_test_tbl;
show partitions mydb.target_test_tbl;

-- hive.use.scratchdir.for.staging=true
-- Staging uses ${hive.exec.scratchdir} entirely outside the table path:
-- <hive.exec.scratchdir>/<staging_dir>/<static_partition>/<dynamic_partition>
-- MoveTask then relocates data from scratchdir staging into <table_path>.
-- If the scratchdir-based staging or MoveTask were broken, the data
-- would NOT appear in the final partition, making this test fail.

set hive.use.scratchdir.for.staging=true;
set hive.exec.stagingdir=/tmp/hive-staging-dir/hive-staging;

insert overwrite table mydb.target_test_tbl partition (created='20250101', day)
select a, '20250101' as day
from mydb.source_test_tbl;

-- Verify data was correctly moved from scratchdir staging to the final partition.
select * from mydb.target_test_tbl;
show partitions mydb.target_test_tbl;

-- File merge validation with hive.merge.tezfiles=true & hive.use.scratchdir.for.staging=true
set tez.grouping.split-count=2;
set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1000000;

insert overwrite table mydb.target_test_tbl partition (created='20250101', day)
select a, '20250101' as day
from mydb.source_test_tbl;

-- Verify data is still correct after merge
select * from mydb.target_test_tbl;
show partitions mydb.target_test_tbl;

-- verify the file count. it should merged into 1 final file by the merge task.
analyze table mydb.target_test_tbl partition(created='20250101', day='20250101') compute statistics noscan;
desc formatted mydb.target_test_tbl partition(created='20250101', day='20250101');

-- Cleanup
set hive.use.scratchdir.for.staging=false;
set hive.merge.tezfiles=false;

drop table mydb.source_test_tbl;
drop table mydb.target_test_tbl;
drop database mydb;

