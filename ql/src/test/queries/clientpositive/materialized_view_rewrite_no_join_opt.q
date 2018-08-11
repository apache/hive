-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.optimize.constraints.join=false;

create table emps_n30 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n30 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (120, 10, 'Bill', 10000, 250);
analyze table emps_n30 compute statistics for columns;

create table depts_n20 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n20 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts_n20 compute statistics for columns;

create table dependents_n20 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n20 values (10, 'Michael'), (20, 'Jane');
analyze table dependents_n20 compute statistics for columns;

create table locations_n20 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n20 values (10, 'San Francisco'), (20, 'San Diego');
analyze table locations_n20 compute statistics for columns;

alter table emps_n30 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n20 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n20 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n20 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n30 add constraint fk1 foreign key (deptno) references depts_n20(deptno) disable novalidate rely;
alter table depts_n20 add constraint fk2 foreign key (locationid) references locations_n20(locationid) disable novalidate rely;

-- EXAMPLE 1
create materialized view mv1_n20 as
select deptno, name, salary, commission
from emps_n30;
analyze table mv1_n20 compute statistics for columns;

explain
select emps_n30.name, emps_n30.salary, emps_n30.commission
from emps_n30
join depts_n20 using (deptno);

select emps_n30.name, emps_n30.salary, emps_n30.commission
from emps_n30
join depts_n20 using (deptno);

drop materialized view mv1_n20;

-- EXAMPLE 2
create materialized view mv1_n20 as
select empid, emps_n30.deptno, count(*) as c, sum(empid) as s
from emps_n30 join depts_n20 using (deptno)
group by empid, emps_n30.deptno;
analyze table mv1_n20 compute statistics for columns;

explain
select depts_n20.deptno, count(*) as c, sum(empid) as s
from emps_n30 join depts_n20 using (deptno)
group by depts_n20.deptno;

select depts_n20.deptno, count(*) as c, sum(empid) as s
from emps_n30 join depts_n20 using (deptno)
group by depts_n20.deptno;

drop materialized view mv1_n20;

-- EXAMPLE 3
create materialized view mv1_n20 as
select dependents_n20.empid, emps_n30.deptno, sum(salary) as s
from emps_n30
join dependents_n20 on (emps_n30.empid = dependents_n20.empid)
group by dependents_n20.empid, emps_n30.deptno;
analyze table mv1_n20 compute statistics for columns;

explain
select dependents_n20.empid, sum(salary) as s
from emps_n30
join depts_n20 on (emps_n30.deptno = depts_n20.deptno)
join dependents_n20 on (emps_n30.empid = dependents_n20.empid)
group by dependents_n20.empid;
 
select dependents_n20.empid, sum(salary) as s
from emps_n30
join depts_n20 on (emps_n30.deptno = depts_n20.deptno)
join dependents_n20 on (emps_n30.empid = dependents_n20.empid)
group by dependents_n20.empid;
 
drop materialized view mv1_n20;

-- EXAMPLE 4
create materialized view mv1_n20 as
select emps_n30.empid, emps_n30.deptno, emps_n30.name as name1, emps_n30.salary, emps_n30.commission, dependents_n20.name as name2
from emps_n30 join dependents_n20 on (emps_n30.empid = dependents_n20.empid);
analyze table mv1_n20 compute statistics for columns;

explain
select emps_n30.empid, dependents_n20.empid, emps_n30.deptno
from emps_n30
join dependents_n20 on (emps_n30.empid = dependents_n20.empid)
join depts_n20 a on (emps_n30.deptno=a.deptno)
where emps_n30.name = 'Bill';

select emps_n30.empid, dependents_n20.empid, emps_n30.deptno
from emps_n30
join dependents_n20 on (emps_n30.empid = dependents_n20.empid)
join depts_n20 a on (emps_n30.deptno=a.deptno)
where emps_n30.name = 'Bill';

drop materialized view mv1_n20;





