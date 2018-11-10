-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n8 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n8 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250);
analyze table emps_n8 compute statistics for columns;

create table depts_n6 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n6 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts_n6 compute statistics for columns;

create table dependents_n4 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n4 values (10, 'Michael'), (20, 'Jane');
analyze table dependents_n4 compute statistics for columns;

create table locations_n4 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n4 values (10, 'San Francisco'), (20, 'San Diego');
analyze table locations_n4 compute statistics for columns;

alter table emps_n8 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n6 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n4 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n4 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n8 add constraint fk1 foreign key (deptno) references depts_n6(deptno) disable novalidate rely;
alter table depts_n6 add constraint fk2 foreign key (locationid) references locations_n4(locationid) disable novalidate rely;

alter table emps_n8 change column deptno deptno int constraint nn1 not null disable novalidate rely;
alter table depts_n6 change column locationid locationid int constraint nn2 not null disable novalidate rely;


-- EXAMPLE 21 -- WORKS NOW
create materialized view mv1_n4 as
select depts_n6.deptno, dependents_n4.empid
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join locations_n4 on (locations_n4.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 11
group by depts_n6.deptno, dependents_n4.empid;
analyze table mv1_n4 compute statistics for columns;

explain
select dependents_n4.empid, depts_n6.deptno
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join locations_n4 on (locations_n4.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 10
group by dependents_n4.empid, depts_n6.deptno;

select dependents_n4.empid, depts_n6.deptno
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join locations_n4 on (locations_n4.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 10
group by dependents_n4.empid, depts_n6.deptno;

drop materialized view mv1_n4;

-- EXAMPLE 33
create materialized view mv1_n4 as
select depts_n6.deptno, dependents_n4.empid, count(emps_n8.salary) as s
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join locations_n4 on (locations_n4.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 11 and depts_n6.deptno < 19
group by depts_n6.deptno, dependents_n4.empid;
analyze table mv1_n4 compute statistics for columns;

explain
select dependents_n4.empid, count(emps_n8.salary) + 1
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join locations_n4 on (locations_n4.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 10 and depts_n6.deptno < 20
group by dependents_n4.empid;

select dependents_n4.empid, count(emps_n8.salary) + 1
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join locations_n4 on (locations_n4.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 10 and depts_n6.deptno < 20
group by dependents_n4.empid;

drop materialized view mv1_n4;

-- EXAMPLE 40 -- REWRITING HAPPENS BUT DISCARDED
-- DUE TO COST EXCEPT WITH HEURISTICS
create materialized view mv1_n4 as
select depts_n6.deptno, dependents_n4.empid
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno >= 10;
analyze table mv1_n4 compute statistics for columns;

explain
select dependents_n4.empid
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 0;

select dependents_n4.empid
from depts_n6
join dependents_n4 on (depts_n6.name = dependents_n4.name)
join emps_n8 on (emps_n8.deptno = depts_n6.deptno)
where depts_n6.deptno > 0;

drop materialized view mv1_n4;
