--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.mapredfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.tezfiles=true;
set hive.blobstore.supported.schemes=hdfs,file;
set hive.merge.smallfiles.avgsize=7500;

-- SORT_QUERY_RESULTS

create table part_source(key string, value string) partitioned by (ds string);
create table source(key string);

-- The partitioned table must have 2 files per partition (necessary for merge task)
insert overwrite table part_source partition(ds='102') select * from src;
insert into table part_source partition(ds='102') select * from src;
insert overwrite table part_source partition(ds='103') select * from src;
insert into table part_source partition(ds='103') select * from src;

-- The unpartitioned table must have 2 files.
insert overwrite table source select key from src;
insert into table source select key from src;

-- Create CTAS tables both for unpartitioned and partitioned cases for ORC formats.
create external table ctas_table stored as orc as select * from source;
create external table ctas_part_table partitioned by (ds) stored as orc as select * from part_source;

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_part_table;

-- Create CTAS tables both for unpartitioned and partitioned cases for non-ORC formats.
create external table ctas_table_non_orc as select * from source;
create external table ctas_part_table_non_orc partitioned by (ds) as select * from part_source;

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_non_orc;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_part_table_non_orc;

-- Create CTAS tables with union both for unpartitioned and partitioned cases for ORC formats.
create external table ctas_table_orc_union stored as orc as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));
create external table ctas_table_part_orc_union partitioned by (ds) stored as orc as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_orc_union;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_part_orc_union;

-- Create CTAS tables with union both for unpartitioned and partitioned cases for non-ORC formats.
create external table ctas_table_non_orc_union as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));
create external table ctas_table_part_non_orc_union partitioned by (ds) as ((select * from part_source where ds = '102') union (select * from part_source where ds = '103'));

-- This must be 1 indicating there is 1 file after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_non_orc_union;
-- This must be 2 indicating there is 1 file per partition after merge.
select count(distinct(INPUT__FILE__NAME)) from ctas_table_part_non_orc_union;
