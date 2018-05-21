-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n5 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n5 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);
analyze table emps_n5 compute statistics for columns;

create table depts_n4 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n4 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts_n4 compute statistics for columns;

create table dependents_n3 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n3 values (10, 'Michael'), (10, 'Jane');
analyze table dependents_n3 compute statistics for columns;

create table locations_n3 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n3 values (10, 'San Francisco'), (10, 'San Diego');
analyze table locations_n3 compute statistics for columns;

alter table emps_n5 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n4 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n3 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n3 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n5 add constraint fk1 foreign key (deptno) references depts_n4(deptno) disable novalidate rely;
alter table depts_n4 add constraint fk2 foreign key (locationid) references locations_n3(locationid) disable novalidate rely;


-- EXAMPLE 10
create materialized view mv1_n3 enable rewrite as
select name, salary, count(*) as c, sum(empid) as s
from emps_n5 group by name, salary;
analyze table mv1_n3 compute statistics for columns;

explain
select name, count(*) as c, sum(empid) as s
from emps_n5 group by name;

select name, count(*) as c, sum(empid) as s
from emps_n5 group by name;

drop materialized view mv1_n3;

-- EXAMPLE 11
create materialized view mv1_n3 enable rewrite as
select name, salary, count(*) as c, sum(empid) as s
from emps_n5 group by name, salary;
analyze table mv1_n3 compute statistics for columns;

explain
select salary, name, sum(empid) as s, count(*) as c
from emps_n5 group by name, salary;

select salary, name, sum(empid) as s, count(*) as c
from emps_n5 group by name, salary;

drop materialized view mv1_n3;

-- EXAMPLE 25
create materialized view mv1_n3 enable rewrite as
select empid, emps_n5.deptno, count(*) as c, sum(empid) as s
from emps_n5 join depts_n4 using (deptno)
group by empid, emps_n5.deptno;
analyze table mv1_n3 compute statistics for columns;

explain
select depts_n4.deptno, count(*) as c, sum(empid) as s
from emps_n5 join depts_n4 using (deptno)
group by depts_n4.deptno;

select depts_n4.deptno, count(*) as c, sum(empid) as s
from emps_n5 join depts_n4 using (deptno)
group by depts_n4.deptno;

drop materialized view mv1_n3;

-- EXAMPLE 27
create materialized view mv1_n3 enable rewrite as
select empid, emps_n5.deptno, count(*) as c, sum(empid) as s
from emps_n5 join depts_n4 using (deptno)
where emps_n5.deptno >= 10 group by empid, emps_n5.deptno;
analyze table mv1_n3 compute statistics for columns;

explain
select depts_n4.deptno, sum(empid) as s
from emps_n5 join depts_n4 using (deptno)
where emps_n5.deptno > 10 group by depts_n4.deptno;

select depts_n4.deptno, sum(empid) as s
from emps_n5 join depts_n4 using (deptno)
where emps_n5.deptno > 10 group by depts_n4.deptno;

drop materialized view mv1_n3;

-- EXAMPLE 28
create materialized view mv1_n3 enable rewrite as
select empid, depts_n4.deptno, count(*) + 1 as c, sum(empid) as s
from emps_n5 join depts_n4 using (deptno)
where depts_n4.deptno >= 10 group by empid, depts_n4.deptno;
analyze table mv1_n3 compute statistics for columns;

explain
select depts_n4.deptno, sum(empid) + 1 as s
from emps_n5 join depts_n4 using (deptno)
where depts_n4.deptno > 10 group by depts_n4.deptno;

select depts_n4.deptno, sum(empid) + 1 as s
from emps_n5 join depts_n4 using (deptno)
where depts_n4.deptno > 10 group by depts_n4.deptno;

drop materialized view mv1_n3;

-- EXAMPLE 29
create materialized view mv1_n3 enable rewrite as
select depts_n4.name, sum(salary) as s
from emps_n5
join depts_n4 on (emps_n5.deptno = depts_n4.deptno)
group by depts_n4.name;
analyze table mv1_n3 compute statistics for columns;

explain
select dependents_n3.empid, sum(salary) as s
from emps_n5
join depts_n4 on (emps_n5.deptno = depts_n4.deptno)
join dependents_n3 on (depts_n4.name = dependents_n3.name)
group by dependents_n3.empid;

select dependents_n3.empid, sum(salary) as s
from emps_n5
join depts_n4 on (emps_n5.deptno = depts_n4.deptno)
join dependents_n3 on (depts_n4.name = dependents_n3.name)
group by dependents_n3.empid;

drop materialized view mv1_n3;

-- EXAMPLE 32
create materialized view mv1_n3 enable rewrite as
select dependents_n3.empid, emps_n5.deptno, count(distinct salary) as s
from emps_n5
join dependents_n3 on (emps_n5.empid = dependents_n3.empid)
group by dependents_n3.empid, emps_n5.deptno;
analyze table mv1_n3 compute statistics for columns;

explain
select emps_n5.deptno, count(distinct salary) as s
from emps_n5
join dependents_n3 on (emps_n5.empid = dependents_n3.empid)
group by dependents_n3.empid, emps_n5.deptno;

select emps_n5.deptno, count(distinct salary) as s
from emps_n5
join dependents_n3 on (emps_n5.empid = dependents_n3.empid)
group by dependents_n3.empid, emps_n5.deptno;

drop materialized view mv1_n3;
