--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.mapredfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.tezfiles=true;
set hive.blobstore.supported.schemes=hdfs,file;

-- SORT_QUERY_RESULTS

create table part_source(key string, value string) partitioned by (ds string);
create table source(key string);
create table one_part_source(key string, value string) partitioned by (ds string);

-- The partitioned table must have 2 files per partition (necessary for merge task)
insert overwrite table part_source partition(ds='102') select * from src;
insert into table part_source partition(ds='102') select * from src;
insert overwrite table part_source partition(ds='103') select * from src;
insert into table part_source partition(ds='103') select * from src;

-- The unpartitioned table must have 2 files.
insert overwrite table source select key from src;
insert into table source select key from src;

-- The partitioned table must have 1 file on one partition and 2 files on another partition (check merge and move task execution)
insert overwrite table one_part_source partition(ds='102') select * from src;
insert overwrite table one_part_source partition(ds='103') select * from src;
insert into table one_part_source partition(ds='103') select * from src;

select count(*) from source;
select count(*) from part_source;
select count(*) from one_part_source;

-- Create CTAS tables both for unpartitioned and partitioned cases for ORC formats.
create external table ctas_table stored as orc as select * from source;
create external table ctas_part_table partitioned by (ds) stored as orc as select * from part_source;
create external table ctas_one_part_table partitioned by (ds) stored as orc as select * from one_part_source;

select count(*) from ctas_table;
select count(*) from ctas_part_table;
select count(*) from ctas_one_part_table;

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_part_table;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_one_part_table;

-- Create CTAS tables both for unpartitioned and partitioned cases for non-ORC formats.
create external table ctas_table_non_orc as select * from source;
create external table ctas_part_table_non_orc partitioned by (ds) as select * from part_source;
create external table ctas_one_part_table_non_orc partitioned by (ds) stored as orc as select * from one_part_source;

select count(*) from ctas_table_non_orc;
select count(*) from ctas_part_table_non_orc;
select count(*) from ctas_one_part_table_non_orc;

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_non_orc;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_part_table_non_orc;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_one_part_table_non_orc;

-- Create CTAS tables with union both for unpartitioned and partitioned cases for ORC formats.
create external table ctas_table_orc_union stored as orc as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));
create external table ctas_table_part_orc_union partitioned by (ds) stored as orc as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));

select count(*) from ((select * from part_source where ds = '102') union (select * from part_source where ds = '103')) count_table;

select count(*) from ctas_table_orc_union;
select count(*) from ctas_table_part_orc_union;

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_orc_union;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_part_orc_union;

-- Create CTAS tables with union both for unpartitioned and partitioned cases for non-ORC formats.
create external table ctas_table_non_orc_union as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));
create external table ctas_table_part_non_orc_union partitioned by (ds) as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));

select count(*) from ctas_table_non_orc_union;
select count(*) from ctas_table_part_non_orc_union;

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_non_orc_union;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_part_non_orc_union;

-- Create CTAS tables with union-all clause for unpartitioned table for both ORC and non-ORC format (Note: This doesn't create the manifest file as of this commit).
create external table ctas_table_orc_union_all stored as orc as ((select * from part_source where ds = '102') union all (select * from part_source where ds = '103'));
create external table ctas_table_non_orc_union_all as ((select * from part_source where ds = '102') union all (select * from part_source where ds = '103'));

select count(*) from ctas_table_orc_union_all;
select count(*) from ctas_table_non_orc_union_all;

-- This must be 1 indicating there is 1 file after merge in both cases.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_orc_union_all;
select count(distinct(INPUT__FILE__NAME)) from ctas_table_non_orc_union_all;
