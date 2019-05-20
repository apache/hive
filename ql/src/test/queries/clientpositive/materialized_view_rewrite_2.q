-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n0 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n0 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);

create table depts_n0 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n0 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);

create table dependents_n0 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n0 values (10, 'Michael'), (10, 'Jane');

create table locations_n0 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n0 values (10, 'San Francisco'), (10, 'San Diego');

alter table emps_n0 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n0 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n0 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n0 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n0 add constraint fk1 foreign key (deptno) references depts_n0(deptno) disable novalidate rely;
alter table depts_n0 add constraint fk2 foreign key (locationid) references locations_n0(locationid) disable novalidate rely;

-- EXAMPLE 16
create materialized view mv1_n0 as
select empid, depts_n0.deptno from emps_n0
join depts_n0 using (deptno) where depts_n0.deptno > 10
group by empid, depts_n0.deptno;

explain
select empid from emps_n0
join depts_n0 using (deptno) where depts_n0.deptno > 20
group by empid, depts_n0.deptno;

select empid from emps_n0
join depts_n0 using (deptno) where depts_n0.deptno > 20
group by empid, depts_n0.deptno;

drop materialized view mv1_n0;

-- EXAMPLE 17
create materialized view mv1_n0 as
select depts_n0.deptno, empid from depts_n0
join emps_n0 using (deptno) where depts_n0.deptno > 10
group by empid, depts_n0.deptno;

explain
select empid from emps_n0
join depts_n0 using (deptno) where depts_n0.deptno > 20
group by empid, depts_n0.deptno;

select empid from emps_n0
join depts_n0 using (deptno) where depts_n0.deptno > 20
group by empid, depts_n0.deptno;

drop materialized view mv1_n0;

-- EXAMPLE 18
create materialized view mv1_n0 as
select empid, depts_n0.deptno from emps_n0
join depts_n0 using (deptno) where emps_n0.deptno > 10
group by empid, depts_n0.deptno;

explain
select empid from emps_n0
join depts_n0 using (deptno) where depts_n0.deptno > 20
group by empid, depts_n0.deptno;

select empid from emps_n0
join depts_n0 using (deptno) where depts_n0.deptno > 20
group by empid, depts_n0.deptno;

drop materialized view mv1_n0;

-- EXAMPLE 19
create materialized view mv1_n0 as
select depts_n0.deptno, emps_n0.empid from depts_n0
join emps_n0 using (deptno) where emps_n0.empid > 10
group by depts_n0.deptno, emps_n0.empid;

explain
select depts_n0.deptno from depts_n0
join emps_n0 using (deptno) where emps_n0.empid > 15
group by depts_n0.deptno, emps_n0.empid;

select depts_n0.deptno from depts_n0
join emps_n0 using (deptno) where emps_n0.empid > 15
group by depts_n0.deptno, emps_n0.empid;

drop materialized view mv1_n0;

-- EXAMPLE 20
create materialized view mv1_n0 as
select depts_n0.deptno, emps_n0.empid from depts_n0
join emps_n0 using (deptno) where emps_n0.empid > 10
group by depts_n0.deptno, emps_n0.empid;

explain
select depts_n0.deptno from depts_n0
join emps_n0 using (deptno) where emps_n0.empid > 15
group by depts_n0.deptno;

select depts_n0.deptno from depts_n0
join emps_n0 using (deptno) where emps_n0.empid > 15
group by depts_n0.deptno;

drop materialized view mv1_n0;

-- EXAMPLE 23
create materialized view mv1_n0 as
select depts_n0.name, dependents_n0.name as name2, emps_n0.deptno, depts_n0.deptno as deptno2, dependents_n0.empid
from depts_n0, dependents_n0, emps_n0
where depts_n0.deptno > 10
group by depts_n0.name, dependents_n0.name, emps_n0.deptno, depts_n0.deptno, dependents_n0.empid;

explain
select dependents_n0.empid
from depts_n0
join dependents_n0 on (depts_n0.name = dependents_n0.name)
join emps_n0 on (emps_n0.deptno = depts_n0.deptno)
where depts_n0.deptno > 10
group by dependents_n0.empid;

select dependents_n0.empid
from depts_n0
join dependents_n0 on (depts_n0.name = dependents_n0.name)
join emps_n0 on (emps_n0.deptno = depts_n0.deptno)
where depts_n0.deptno > 10
group by dependents_n0.empid;

drop materialized view mv1_n0;
