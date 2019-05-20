set hive.stats.column.autogather=false;
set hive.vectorized.execution.enabled=true;
create temporary table foo (x int) clustered by (x) into 4 buckets stored as orc;
explain vectorization detail insert overwrite table foo values(1),(2),(3),(4),(9);
insert overwrite table foo values(1),(2),(3),(4),(9);
select *, regexp_extract(INPUT__FILE__NAME, '.*/(.*)', 1) from foo;
drop table foo;
