-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n2 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n2 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);
analyze table emps_n2 compute statistics for columns;

create table depts_n1 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n1 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts_n1 compute statistics for columns;

create table dependents_n1 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n1 values (10, 'Michael'), (10, 'Jane');
analyze table dependents_n1 compute statistics for columns;

create table locations_n1 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n1 values (10, 'San Francisco'), (10, 'San Diego');
analyze table locations_n1 compute statistics for columns;

alter table emps_n2 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n1 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n1 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n1 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n2 add constraint fk1 foreign key (deptno) references depts_n1(deptno) disable novalidate rely;
alter table depts_n1 add constraint fk2 foreign key (locationid) references locations_n1(locationid) disable novalidate rely;

alter table emps_n2 change column deptno deptno int constraint nn1 not null disable novalidate rely;
alter table depts_n1 change column locationid locationid int constraint nn2 not null disable novalidate rely;


-- EXAMPLE 8
create materialized view mv1_n1 enable rewrite as
select name, deptno, salary from emps_n2 where deptno > 15 group by name, deptno, salary;
analyze table mv1_n1 compute statistics for columns;

explain
select name from emps_n2 where deptno >= 20 group by name;

select name from emps_n2 where deptno >= 20 group by name;

drop materialized view mv1_n1;

-- EXAMPLE 12
create materialized view mv1_n1 enable rewrite as
select name, deptno, salary, count(*) as c, sum(empid) as s
from emps_n2 where deptno >= 15 group by name, deptno, salary;
analyze table mv1_n1 compute statistics for columns;

explain
select name, sum(empid) as s
from emps_n2 where deptno > 15 group by name;

select name, sum(empid) as s
from emps_n2 where deptno > 15 group by name;

drop materialized view mv1_n1;

-- EXAMPLE 22
create materialized view mv1_n1 enable rewrite as
select depts_n1.deptno, dependents_n1.empid
from depts_n1
join dependents_n1 on (depts_n1.name = dependents_n1.name)
join locations_n1 on (locations_n1.name = dependents_n1.name)
join emps_n2 on (emps_n2.deptno = depts_n1.deptno)
where depts_n1.deptno > 10 and depts_n1.deptno < 20
group by depts_n1.deptno, dependents_n1.empid;
analyze table mv1_n1 compute statistics for columns;

explain
select dependents_n1.empid
from depts_n1
join dependents_n1 on (depts_n1.name = dependents_n1.name)
join locations_n1 on (locations_n1.name = dependents_n1.name)
join emps_n2 on (emps_n2.deptno = depts_n1.deptno)
where depts_n1.deptno > 11 and depts_n1.deptno < 19
group by dependents_n1.empid;

select dependents_n1.empid
from depts_n1
join dependents_n1 on (depts_n1.name = dependents_n1.name)
join locations_n1 on (locations_n1.name = dependents_n1.name)
join emps_n2 on (emps_n2.deptno = depts_n1.deptno)
where depts_n1.deptno > 11 and depts_n1.deptno < 19
group by dependents_n1.empid;

drop materialized view mv1_n1;

-- EXAMPLE 24
create materialized view mv1_n1 enable rewrite as
select empid, depts_n1.deptno, count(*) as c, sum(empid) as s
from emps_n2 join depts_n1 using (deptno)
group by empid, depts_n1.deptno;
analyze table mv1_n1 compute statistics for columns;

explain
select deptno from emps_n2 group by deptno;

select deptno from emps_n2 group by deptno;

drop materialized view mv1_n1;

-- EXAMPLE 26
create materialized view mv1_n1 enable rewrite as
select empid, depts_n1.deptno, count(*) as c, sum(empid) as s
from emps_n2 join depts_n1 using (deptno)
group by empid, depts_n1.deptno;
analyze table mv1_n1 compute statistics for columns;

explain
select deptno, empid, sum(empid) as s, count(*) as c
from emps_n2 group by empid, deptno;

select deptno, empid, sum(empid) as s, count(*) as c
from emps_n2 group by empid, deptno;

drop materialized view mv1_n1;

-- EXAMPLE 30
create materialized view mv1_n1 enable rewrite as
select dependents_n1.empid, emps_n2.deptno, sum(salary) as s
from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
group by dependents_n1.empid, emps_n2.deptno;
analyze table mv1_n1 compute statistics for columns;

explain
select dependents_n1.empid, sum(salary) as s
from emps_n2
join depts_n1 on (emps_n2.deptno = depts_n1.deptno)
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
group by dependents_n1.empid;

select dependents_n1.empid, sum(salary) as s
from emps_n2
join depts_n1 on (emps_n2.deptno = depts_n1.deptno)
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
group by dependents_n1.empid;

drop materialized view mv1_n1;

-- EXAMPLE 31
create materialized view mv1_n1 enable rewrite as
select dependents_n1.empid, emps_n2.deptno, sum(salary) as s
from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
group by dependents_n1.empid, emps_n2.deptno;
analyze table mv1_n1 compute statistics for columns;

explain
select depts_n1.name, sum(salary) as s
from emps_n2
join depts_n1 on (emps_n2.deptno = depts_n1.deptno)
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
group by depts_n1.name;

select depts_n1.name, sum(salary) as s
from emps_n2
join depts_n1 on (emps_n2.deptno = depts_n1.deptno)
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
group by depts_n1.name;

drop materialized view mv1_n1;

-- EXAMPLE 41
create materialized view mv1_n1 enable rewrite as
select a.empid deptno from
(select * from emps_n2 where empid = 1) a
join depts_n1 on (a.deptno = depts_n1.deptno)
join dependents_n1 on (a.empid = dependents_n1.empid);
analyze table mv1_n1 compute statistics for columns;

explain
select a.empid from 
(select * from emps_n2 where empid = 1) a
join dependents_n1 on (a.empid = dependents_n1.empid);

select a.empid from 
(select * from emps_n2 where empid = 1) a
join dependents_n1 on (a.empid = dependents_n1.empid);

drop materialized view mv1_n1;

-- EXAMPLE 42
create materialized view mv1_n1 enable rewrite as
select a.empid, a.deptno from
(select * from emps_n2 where empid = 1) a
join depts_n1 on (a.deptno = depts_n1.deptno)
join dependents_n1 on (a.empid = dependents_n1.empid);
analyze table mv1_n1 compute statistics for columns;

explain
select a.empid from 
(select * from emps_n2 where empid = 1) a
join dependents_n1 on (a.empid = dependents_n1.empid);

select a.empid from 
(select * from emps_n2 where empid = 1) a
join dependents_n1 on (a.empid = dependents_n1.empid);

drop materialized view mv1_n1;

-- EXAMPLE 43
create materialized view mv1_n1 enable rewrite as
select empid deptno from
(select * from emps_n2 where empid = 1) a
join depts_n1 on (a.deptno = depts_n1.deptno);
analyze table mv1_n1 compute statistics for columns;

explain
select empid from emps_n2 where empid = 1;

select empid from emps_n2 where empid = 1;

drop materialized view mv1_n1;

-- EXAMPLE 44
create materialized view mv1_n1 enable rewrite as
select emps_n2.empid, emps_n2.deptno from emps_n2
join depts_n1 on (emps_n2.deptno = depts_n1.deptno)
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.empid = 1;
analyze table mv1_n1 compute statistics for columns;

explain
select emps_n2.empid from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.empid = 1;

select emps_n2.empid from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.empid = 1;

drop materialized view mv1_n1;

-- EXAMPLE 45a
create materialized view mv1_n1 enable rewrite as
select emps_n2.empid, emps_n2.deptno from emps_n2
join depts_n1 a on (emps_n2.deptno=a.deptno)
join depts_n1 b on (emps_n2.deptno=b.deptno)
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.empid = 1;
analyze table mv1_n1 compute statistics for columns;

explain
select emps_n2.empid from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.empid = 1;

select emps_n2.empid from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.empid = 1;

drop materialized view mv1_n1;

-- EXAMPLE 45b
create materialized view mv1_n1 enable rewrite as
select emps_n2.empid, emps_n2.deptno from emps_n2
join depts_n1 a on (emps_n2.deptno=a.deptno)
join depts_n1 b on (emps_n2.deptno=b.deptno)
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.name = 'Sebastian';
analyze table mv1_n1 compute statistics for columns;

explain
select emps_n2.empid from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.name = 'Sebastian';

select emps_n2.empid from emps_n2
join dependents_n1 on (emps_n2.empid = dependents_n1.empid)
where emps_n2.name = 'Sebastian';

drop materialized view mv1_n1;
