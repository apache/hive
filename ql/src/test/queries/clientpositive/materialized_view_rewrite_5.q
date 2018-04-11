-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);
analyze table emps compute statistics for columns;

create table depts (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts compute statistics for columns;

create table dependents (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents values (10, 'Michael'), (10, 'Jane');
analyze table dependents compute statistics for columns;

create table locations (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations values (10, 'San Francisco'), (10, 'San Diego');
analyze table locations compute statistics for columns;

alter table emps add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps add constraint fk1 foreign key (deptno) references depts(deptno) disable novalidate rely;
alter table depts add constraint fk2 foreign key (locationid) references locations(locationid) disable novalidate rely;

alter table emps change column deptno deptno int constraint nn1 not null disable novalidate rely;
alter table depts change column locationid locationid int constraint nn2 not null disable novalidate rely;


-- EXAMPLE 8
create materialized view mv1 enable rewrite as
select name, deptno, salary from emps where deptno > 15 group by name, deptno, salary;
analyze table mv1 compute statistics for columns;

explain
select name from emps where deptno >= 20 group by name;

select name from emps where deptno >= 20 group by name;

drop materialized view mv1;

-- EXAMPLE 12
create materialized view mv1 enable rewrite as
select name, deptno, salary, count(*) as c, sum(empid) as s
from emps where deptno >= 15 group by name, deptno, salary;
analyze table mv1 compute statistics for columns;

explain
select name, sum(empid) as s
from emps where deptno > 15 group by name;

select name, sum(empid) as s
from emps where deptno > 15 group by name;

drop materialized view mv1;

-- EXAMPLE 22
create materialized view mv1 enable rewrite as
select depts.deptno, dependents.empid
from depts
join dependents on (depts.name = dependents.name)
join locations on (locations.name = dependents.name)
join emps on (emps.deptno = depts.deptno)
where depts.deptno > 10 and depts.deptno < 20
group by depts.deptno, dependents.empid;
analyze table mv1 compute statistics for columns;

explain
select dependents.empid
from depts
join dependents on (depts.name = dependents.name)
join locations on (locations.name = dependents.name)
join emps on (emps.deptno = depts.deptno)
where depts.deptno > 11 and depts.deptno < 19
group by dependents.empid;

select dependents.empid
from depts
join dependents on (depts.name = dependents.name)
join locations on (locations.name = dependents.name)
join emps on (emps.deptno = depts.deptno)
where depts.deptno > 11 and depts.deptno < 19
group by dependents.empid;

drop materialized view mv1;

-- EXAMPLE 24
create materialized view mv1 enable rewrite as
select empid, depts.deptno, count(*) as c, sum(empid) as s
from emps join depts using (deptno)
group by empid, depts.deptno;
analyze table mv1 compute statistics for columns;

explain
select deptno from emps group by deptno;

select deptno from emps group by deptno;

drop materialized view mv1;

-- EXAMPLE 26
create materialized view mv1 enable rewrite as
select empid, depts.deptno, count(*) as c, sum(empid) as s
from emps join depts using (deptno)
group by empid, depts.deptno;
analyze table mv1 compute statistics for columns;

explain
select deptno, empid, sum(empid) as s, count(*) as c
from emps group by empid, deptno;

select deptno, empid, sum(empid) as s, count(*) as c
from emps group by empid, deptno;

drop materialized view mv1;

-- EXAMPLE 30
create materialized view mv1 enable rewrite as
select dependents.empid, emps.deptno, sum(salary) as s
from emps
join dependents on (emps.empid = dependents.empid)
group by dependents.empid, emps.deptno;
analyze table mv1 compute statistics for columns;

explain
select dependents.empid, sum(salary) as s
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (emps.empid = dependents.empid)
group by dependents.empid;

select dependents.empid, sum(salary) as s
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (emps.empid = dependents.empid)
group by dependents.empid;

drop materialized view mv1;

-- EXAMPLE 31
create materialized view mv1 enable rewrite as
select dependents.empid, emps.deptno, sum(salary) as s
from emps
join dependents on (emps.empid = dependents.empid)
group by dependents.empid, emps.deptno;
analyze table mv1 compute statistics for columns;

explain
select depts.name, sum(salary) as s
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (emps.empid = dependents.empid)
group by depts.name;

select depts.name, sum(salary) as s
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (emps.empid = dependents.empid)
group by depts.name;

drop materialized view mv1;

-- EXAMPLE 41
create materialized view mv1 enable rewrite as
select a.empid deptno from
(select * from emps where empid = 1) a
join depts on (a.deptno = depts.deptno)
join dependents on (a.empid = dependents.empid);
analyze table mv1 compute statistics for columns;

explain
select a.empid from 
(select * from emps where empid = 1) a
join dependents on (a.empid = dependents.empid);

select a.empid from 
(select * from emps where empid = 1) a
join dependents on (a.empid = dependents.empid);

drop materialized view mv1;

-- EXAMPLE 42
create materialized view mv1 enable rewrite as
select a.empid, a.deptno from
(select * from emps where empid = 1) a
join depts on (a.deptno = depts.deptno)
join dependents on (a.empid = dependents.empid);
analyze table mv1 compute statistics for columns;

explain
select a.empid from 
(select * from emps where empid = 1) a
join dependents on (a.empid = dependents.empid);

select a.empid from 
(select * from emps where empid = 1) a
join dependents on (a.empid = dependents.empid);

drop materialized view mv1;

-- EXAMPLE 43
create materialized view mv1 enable rewrite as
select empid deptno from
(select * from emps where empid = 1) a
join depts on (a.deptno = depts.deptno);
analyze table mv1 compute statistics for columns;

explain
select empid from emps where empid = 1;

select empid from emps where empid = 1;

drop materialized view mv1;

-- EXAMPLE 44
create materialized view mv1 enable rewrite as
select emps.empid, emps.deptno from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (emps.empid = dependents.empid)
where emps.empid = 1;
analyze table mv1 compute statistics for columns;

explain
select emps.empid from emps
join dependents on (emps.empid = dependents.empid)
where emps.empid = 1;

select emps.empid from emps
join dependents on (emps.empid = dependents.empid)
where emps.empid = 1;

drop materialized view mv1;

-- EXAMPLE 45a
create materialized view mv1 enable rewrite as
select emps.empid, emps.deptno from emps
join depts a on (emps.deptno=a.deptno)
join depts b on (emps.deptno=b.deptno)
join dependents on (emps.empid = dependents.empid)
where emps.empid = 1;
analyze table mv1 compute statistics for columns;

explain
select emps.empid from emps
join dependents on (emps.empid = dependents.empid)
where emps.empid = 1;

select emps.empid from emps
join dependents on (emps.empid = dependents.empid)
where emps.empid = 1;

drop materialized view mv1;

-- EXAMPLE 45b
create materialized view mv1 enable rewrite as
select emps.empid, emps.deptno from emps
join depts a on (emps.deptno=a.deptno)
join depts b on (emps.deptno=b.deptno)
join dependents on (emps.empid = dependents.empid)
where emps.name = 'Sebastian';
analyze table mv1 compute statistics for columns;

explain
select emps.empid from emps
join dependents on (emps.empid = dependents.empid)
where emps.name = 'Sebastian';

select emps.empid from emps
join dependents on (emps.empid = dependents.empid)
where emps.name = 'Sebastian';

drop materialized view mv1;
