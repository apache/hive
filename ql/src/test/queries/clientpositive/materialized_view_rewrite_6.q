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
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (120, 10, 'Bill', 10000, 250);

create table depts (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);

create table dependents (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents values (10, 'Michael'), (20, 'Jane');

create table locations (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations values (10, 'San Francisco'), (20, 'San Diego');

alter table emps add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps add constraint fk1 foreign key (deptno) references depts(deptno) disable novalidate rely;
alter table depts add constraint fk2 foreign key (locationid) references locations(locationid) disable novalidate rely;

alter table emps change column deptno deptno int constraint nn1 not null disable novalidate rely;
alter table depts change column locationid locationid int constraint nn2 not null disable novalidate rely;


-- EXAMPLE 13
create materialized view mv1 as
select name, deptno, salary, count(*) + 1 as c, sum(empid) as s
from emps where deptno >= 10 group by name, deptno, salary;

explain
select salary, sum(empid) + 1 as s
from emps where deptno > 10 group by salary;

select salary, sum(empid) + 1 as s
from emps where deptno > 10 group by salary;

drop materialized view mv1;

-- EXAMPLE 14
create materialized view mv1 as
select name, deptno, salary, count(*) + 1 as c, sum(empid) as s
from emps where deptno >= 15 group by name, deptno, salary;

explain
select salary + 1, sum(empid) + 1 as s
from emps where deptno > 15 group by salary;

select salary + 1, sum(empid) + 1 as s
from emps where deptno > 15 group by salary;

drop materialized view mv1;

-- EXAMPLE 37
create materialized view mv1 as
select depts.name
from emps
join depts on (emps.deptno = depts.deptno);

explain
select dependents.empid
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (depts.name = dependents.name);

select dependents.empid
from emps
join depts on (emps.deptno = depts.deptno)
join dependents on (depts.name = dependents.name);

drop materialized view mv1;

-- EXAMPLE 39
create materialized view mv1 as
select depts.name
from emps
join depts on (emps.deptno = depts.deptno);

explain
select dependents.empid
from depts
join dependents on (depts.name = dependents.name)
join locations on (locations.name = dependents.name)
join emps on (emps.deptno = depts.deptno);

select dependents.empid
from depts
join dependents on (depts.name = dependents.name)
join locations on (locations.name = dependents.name)
join emps on (emps.deptno = depts.deptno);

drop materialized view mv1;

-- EXAMPLE 46
create materialized view mv1 as
select emps.empid, emps.deptno, emps.name as name1, emps.salary, emps.commission, dependents.name as name2
from emps join dependents on (emps.empid = dependents.empid);

explain
select emps.empid, dependents.empid, emps.deptno
from emps
join dependents on (emps.empid = dependents.empid)
join depts a on (emps.deptno=a.deptno)
where emps.name = 'Bill';

select emps.empid, dependents.empid, emps.deptno
from emps
join dependents on (emps.empid = dependents.empid)
join depts a on (emps.deptno=a.deptno)
where emps.name = 'Bill';

drop materialized view mv1;
