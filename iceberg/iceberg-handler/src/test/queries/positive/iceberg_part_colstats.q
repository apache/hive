--! qt:replace:/(\s+Statistics\: Num rows\: \d+ Data size\:\s+)\S+(\s+Basic stats\: \S+ Column stats\: \S+)/$1#Masked#$2/

-- Column statistics kept per partition answer a query over the partitions it pruned to, and stop
-- answering only for the partitions a later write reached.

set hive.explain.user=false;
set hive.compute.query.using.stats=true;
set hive.fetch.task.conversion=none;
set hive.iceberg.stats.collect.partlevel=true;

create external table ice_part_stats (id bigint, p string)
    partitioned by spec (p)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_part_stats values (1, 'a'), (9, 'a'), (7, 'b'), (3, 'c');
analyze table ice_part_stats compute statistics for columns;

-- answered from the statistics of the pruned partition alone
explain
select max(id) from ice_part_stats where p = 'a';

select max(id) from ice_part_stats where p = 'a';

-- a write that reaches p=a only
insert into ice_part_stats values (11, 'a');

-- a scan spanning the partition the write reached and one it did not has statistics for only part
-- of what it reads, which is what PARTIAL says
explain
select id from ice_part_stats where p in ('a', 'b');

-- p=a describes itself no longer, so the query has to read it
explain
select max(id) from ice_part_stats where p = 'a';

select max(id) from ice_part_stats where p = 'a';

-- the partitions that write never touched still answer from their statistics
explain
select max(id) from ice_part_stats where p = 'b';

select max(id) from ice_part_stats where p = 'b';

-- an ANALYZE naming the written partition measures it again, and it answers from statistics once
-- more while the partitions carried across that ANALYZE keep the numbers they were computed with
analyze table ice_part_stats partition (p = 'a') compute statistics for columns;

explain
select max(id) from ice_part_stats where p = 'a';

select max(id) from ice_part_stats where p = 'a';

explain
select max(id) from ice_part_stats where p = 'b';

select max(id) from ice_part_stats where p = 'b';

-- count(col) needs a row count as well as the column's null count, and a handler keeps no
-- partition parameters to read one from: it is asked of the table for the pruned partitions
explain
select count(id) from ice_part_stats where p = 'b';

select count(id) from ice_part_stats where p = 'b';

-- a query spanning a written and an untouched partition cannot be answered from a subset
explain
select max(id) from ice_part_stats where p in ('a', 'b');

select max(id) from ice_part_stats where p in ('a', 'b');

drop table ice_part_stats;

-- an unpartitioned table keeps its statistics in the same file, which the metastore never holds:
-- reaching them takes the handler, and only the accuracy check stands between a query and stale ones
create external table ice_unpart (id bigint)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_unpart values (1), (5), (9);
analyze table ice_unpart compute statistics for columns;

explain
select max(id) from ice_unpart;

select max(id) from ice_unpart;

-- an incremental gather keeps them describing the table, so it still answers
insert into ice_unpart values (11);

explain
select max(id) from ice_unpart;

select max(id) from ice_unpart;

-- a write that records nothing, as another engine's would, leaves them behind: only the accuracy
-- check stands between the query and a value the table no longer holds
set hive.stats.autogather=false;
insert into ice_unpart values (20);
set hive.stats.autogather=true;

explain
select max(id) from ice_unpart;

select max(id) from ice_unpart;

drop table ice_unpart;

-- statistics kept for the table as a whole describe no partition in particular, so a query over
-- one of them cannot be answered from them however fresh they are
set hive.iceberg.stats.collect.partlevel=false;

create external table ice_tbl_level (id bigint, p string)
    partitioned by spec (p)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_tbl_level values (1, 'a'), (9, 'a'), (7, 'b');
analyze table ice_tbl_level compute statistics for columns;

-- a scan reading every partition reads the whole table, which is what they do describe
explain
select max(id) from ice_tbl_level;

select max(id) from ice_tbl_level;

explain
select count(id) from ice_tbl_level;

select count(id) from ice_tbl_level;

explain
select max(id) from ice_tbl_level where p = 'a';

select max(id) from ice_tbl_level where p = 'a';

drop table ice_tbl_level;

set hive.iceberg.stats.collect.partlevel=true;

-- with the statistics kept by the metastore there are no per-partition numbers to answer from
set hive.iceberg.stats.source=metastore;

create external table ice_part_stats_hms (id bigint, p string)
    partitioned by spec (p)
stored by iceberg tblproperties ('format-version'='2');

insert into ice_part_stats_hms values (1, 'a'), (9, 'a'), (7, 'b');

explain
select max(id) from ice_part_stats_hms where p = 'a';

select max(id) from ice_part_stats_hms where p = 'a';

drop table ice_part_stats_hms;
