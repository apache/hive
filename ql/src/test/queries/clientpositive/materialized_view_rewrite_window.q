SET hive.cli.errors.ignore=true;
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET metastore.strict.managed.tables=true;
SET hive.default.fileformat=textfile;
SET hive.default.fileformat.managed=orc;
SET metastore.create.as.acid=true;
SET hive.groupby.position.alias=true;
set hive.materializedview.rewriting.sql=false;

drop database if exists arc_view cascade;
create database arc_view;
use arc_view;

create table wealth (name string, net_worth decimal(15,3), watches string) TBLPROPERTIES ('transactional'='true');
insert into wealth values ('solomon', 2000000000.00, 'simpsons'),('midas', 1000000000.00, 'super bowl'),('midas', 1000000000.00, 'scream'),('midas', 1000000000.00, 'scream2'),('midas', 1000000000.00, 'scream3');
create table tv_view_data (program string, total_views bigint) TBLPROPERTIES ('transactional'='true');
insert into tv_view_data values ('simpsons', 1300000), ('bbt',2700000), ('super bowl',15000000);

explain cbo
select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile;

create materialized view mv_tv_view_data_av1 stored as orc TBLPROPERTIES ('transactional'='true') as
select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile;

explain cbo
select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile
order by quartile;

select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile
order by quartile;

insert into tv_view_data values ('scream', 20000000);

set hive.materializedview.rebuild.incremental=false;

explain
alter materialized view mv_tv_view_data_av1 rebuild;

alter materialized view mv_tv_view_data_av1 rebuild;

explain cbo
select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile
order by quartile;

select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile
order by quartile;

insert into tv_view_data values ('scream2', 30000000);

set hive.materializedview.rebuild.incremental=true;

explain
alter materialized view mv_tv_view_data_av1 rebuild;

alter materialized view mv_tv_view_data_av1 rebuild;

explain cbo
select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile
order by quartile;

select
  t.quartile,
  max(t.total_views) total
from wealth t2,
(select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data) t
where t.program=t2.watches
group by quartile
order by quartile;

drop materialized view mv_tv_view_data_av1;

create materialized view mv_tv_view_data_av2 stored as orc TBLPROPERTIES ('transactional'='true') as
select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data;

insert into tv_view_data values ('scream3', 40000000);

select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data;

explain
alter materialized view mv_tv_view_data_av2 rebuild;

alter materialized view mv_tv_view_data_av2 rebuild;

select
  total_views `total_views`,
  sum(cast(1.5 as decimal(9,4))) over (order by total_views) as quartile,
  program
from tv_view_data;

drop materialized view mv_tv_view_data_av2;
