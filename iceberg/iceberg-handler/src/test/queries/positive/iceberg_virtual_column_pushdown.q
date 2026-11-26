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
-- NOT of an AND, which Hive turns into an OR
select a from ice_vc where not (a = 1 and partition__spec__id = 1) order by a;

-- under BETWEEN: leaving out the column operand used to fail the query
select a from ice_vc where partition__spec__id between 0 and 1 order by a;

-- under AND: the rest still prunes
select a from ice_vc where a = 1 and file__path is not null order by a;

set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter;

-- Each insert writes one file, so a = 1 matches the whole first file and a metadata delete could drop it.
create table ice_vc_del (a int) stored by iceberg stored as parquet tblproperties ('format-version'='2');
insert into ice_vc_del values (1);
insert into ice_vc_del values (2);

-- under AND: no row has spec id 1, so both rows remain
delete from ice_vc_del where a = 1 and partition__spec__id = 1;
select a from ice_vc_del order by a;

-- under OR: every row qualifies, so no row remains
delete from ice_vc_del where a = 1 or partition__spec__id = 0;
select a from ice_vc_del order by a;

create table ice_vc_cow (a int) stored by iceberg stored as parquet
  tblproperties ('format-version'='2', 'write.delete.mode'='copy-on-write');
insert into ice_vc_cow values (1);
insert into ice_vc_cow values (2);

-- copy-on-write, under AND: no row has spec id 1, so both rows remain
delete from ice_vc_cow where a = 1 and partition__spec__id = 1;
select a from ice_vc_cow order by a;

create table ice_vc_upd (a int, b string) stored by iceberg stored as parquet tblproperties ('format-version'='2');
insert into ice_vc_upd values (1, 'x');
insert into ice_vc_upd values (2, 'x');

-- under OR: every row qualifies, so both rows are updated
update ice_vc_upd set b = 'y' where a = 1 or partition__spec__id = 0;
select a, b from ice_vc_upd order by a;
