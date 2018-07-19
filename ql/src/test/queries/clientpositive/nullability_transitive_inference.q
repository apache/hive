-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n6 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n6 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);
analyze table emps_n6 compute statistics for columns;

create table depts_n5 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n5 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts_n5 compute statistics for columns;

alter table emps_n6 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n5 add constraint pk2 primary key (deptno) disable novalidate rely;

alter table emps_n6 add constraint fk1 foreign key (deptno) references depts_n5(deptno) disable novalidate rely;

explain
select empid from emps_n6
join depts_n5 using (deptno) where depts_n5.deptno >= 20
group by empid, depts_n5.deptno;

select empid from emps_n6
join depts_n5 using (deptno) where depts_n5.deptno >= 20
group by empid, depts_n5.deptno;
