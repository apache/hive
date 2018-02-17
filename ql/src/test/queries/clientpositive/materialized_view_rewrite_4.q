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


-- EXAMPLE 10
create materialized view mv1 enable rewrite as
select name, salary, count(*) as c, sum(empid) as s
from emps group by name, salary;
analyze table mv1 compute statistics for columns;

explain
select name, count(*) as c, sum(empid) as s
from emps group by name;

select name, count(*) as c, sum(empid) as s
from emps group by name;

drop materialized view mv1;

-- EXAMPLE 11
create materialized view mv1 enable rewrite as
select name, salary, count(*) as c, sum(empid) as s
from emps group by name, salary;
analyze table mv1 compute statistics for columns;

explain
select salary, name, sum(empid) as s, count(*) as c
from emps group by name, salary;

select salary, name, sum(empid) as s, count(*) as c
from emps group by name, salary;

drop materialized view mv1;

-- EXAMPLE 25
create materialized view mv1 enable rewrite as
select empid, emps.deptno, count(*) as c, sum(empid) as s
from emps join depts using (deptno)
group by empid, emps.deptno;
analyze table mv1 compute statistics for columns;

explain
select depts.deptno, count(*) as c, sum(empid) as s
from emps join depts using (deptno)
group by depts.deptno;

select depts.deptno, count(*) as c, sum(empid) as s
from emps join depts using (deptno)
group by depts.deptno;

drop materialized view mv1;

-- EXAMPLE 27
create materialized view mv1 enable rewrite as
select empid, emps.deptno, count(*) as c, sum(empid) as s
from emps join depts using (deptno)
where emps.deptno >= 10 group by empid, emps.deptno;
analyze table mv1 compute statistics for columns;

explain
select depts.deptno, sum(empid) as s
from emps join depts using (deptno)
where emps.deptno > 10 group by depts.deptno;

select depts.deptno, sum(empid) as s
from emps join depts using (deptno)
where emps.deptno > 10 group by depts.deptno;

drop materialized view mv1;

-- EXAMPLE 28
create materialized view mv1 enable rewrite as
select empid, depts.deptno, count(*) + 1 as c, sum(empid) as s
from emps join depts using (deptno)
where depts.deptno >= 10 group by empid, depts.deptno;
analyze table mv1 compute statistics for columns;

explain
select depts.deptno, sum(empid) + 1 as s
from emps join depts using (deptno)
where depts.deptno > 10 group by depts.deptno;

select depts.deptno, sum(empid) + 1 as s
from emps join depts using (deptno)
where depts.deptno > 10 group by depts.deptno;

drop materialized view mv1;

-- EXAMPLE 29
create materialized view mv1 enable rewrite as
select depts.name, sum(salary) as s
from emps
join depts on (emps.deptno = depts.deptno)
group by depts.name;
analyze table mv1 compute statistics for columns;

explain
select dependents.empid, sum(salary) as s
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (depts.name = dependents.name)
group by dependents.empid;

select dependents.empid, sum(salary) as s
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (depts.name = dependents.name)
group by dependents.empid;

drop materialized view mv1;

-- EXAMPLE 32
create materialized view mv1 enable rewrite as
select dependents.empid, emps.deptno, count(distinct salary) as s
from emps
join dependents on (emps.empid = dependents.empid)
group by dependents.empid, emps.deptno;
analyze table mv1 compute statistics for columns;

explain
select emps.deptno, count(distinct salary) as s
from emps
join dependents on (emps.empid = dependents.empid)
group by dependents.empid, emps.deptno;

select emps.deptno, count(distinct salary) as s
from emps
join dependents on (emps.empid = dependents.empid)
group by dependents.empid, emps.deptno;

drop materialized view mv1;
