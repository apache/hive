-- The incremental rebuild of a V3 materialized view deletes its rewritten groups through DV writers: all the deletes of
-- a data file reach one task (DISTRIBUTE BY FILE__PATH), so the commit gets one DV per data file and merges none, with
-- several reducers in each branch
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rebuild.incremental.factor=0.01;
set hive.fetch.task.conversion=none;

create external table mv3_src (a int, b int) stored by iceberg tblproperties ('format-version'='3');
insert into mv3_src select pos, pos % 50 from (select posexplode(split(space(999), ' '))) t;
create materialized view mv3 stored by iceberg tblproperties ('format-version'='3') as
select b, count(*) c, sum(a) s from mv3_src group by b;
insert into mv3_src select pos + 1000, pos % 50 from (select posexplode(split(space(99), ' '))) t;

set hive.exec.reducers.bytes.per.reducer=256;
set hive.exec.reducers.max=8;
set mapreduce.job.reduces=4;
explain alter materialized view mv3 rebuild;
alter materialized view mv3 rebuild;
select count(*), count(distinct file_path) from default.mv3.delete_files;
select count(*) from default.mv3.delete_files where file_path like '%merged-dvs%';
select sum(c), sum(s), count(*) from mv3;
select sum(c), sum(s), count(distinct b) from (select b, count(*) c, sum(a) s from mv3_src group by b) t;
drop materialized view mv3;
drop table mv3_src;
