-- The filter Hive pushes to an Iceberg table prunes its data files; a virtual column cannot be evaluated there.
-- Each insert writes one file, and RECORDS_IN_Map_1 counts the rows of the files that were read.
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

create external table ice_vc (a int) stored by iceberg stored as parquet;
insert into ice_vc values (1), (2);
insert into ice_vc values (10), (11);
insert into ice_vc values (20), (21);

explain cbo select a from ice_vc where a = 1 or file__path is not null;

set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;

-- under OR: every row qualifies, so every file must be read
select a from ice_vc where a = 1 or file__path is not null order by a;
select a from ice_vc where a = 1 or partition__spec__id = 0 order by a;

-- under AND: the rest still prunes
select a from ice_vc where a = 1 and file__path is not null order by a;
