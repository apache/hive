set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_imp0 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

create table depts_imp0 (
  deptno int,
  name varchar(256),
  locationid int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

alter table emps_imp0 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_imp0 add constraint pk2 primary key (deptno) disable novalidate rely;

alter table emps_imp0 add constraint fk1 foreign key (deptno) references depts_imp0(deptno) disable novalidate rely;

explain extended
create table tab1_imp0
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select empid, depts_imp0.deptno deptno0, count(*) cnt from emps_imp0
join depts_imp0 using (deptno) where depts_imp0.deptno > cast(ltrim('10', 'a') as integer)
group by empid, depts_imp0.deptno;

explain extended
create materialized view mv_tab1_imp0
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select empid, depts_imp0.deptno deptno0, count(*) cnt from emps_imp0
join depts_imp0 using (deptno) where depts_imp0.deptno > cast(ltrim('10', 'a') as integer)
group by empid, depts_imp0.deptno;

explain extended
create view v_tab1_imp0 as
select empid, depts_imp0.deptno deptno0, count(*) cnt from emps_imp0
join depts_imp0 using (deptno) where depts_imp0.deptno > cast(ltrim('10', 'a') as integer)
group by empid, depts_imp0.deptno;

drop table emps_imp0;
drop table depts_imp0;
