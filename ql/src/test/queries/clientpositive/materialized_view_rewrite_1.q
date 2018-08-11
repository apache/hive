-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n3 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n3 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (120, 10, 'Bill', 10000, 250);
analyze table emps_n3 compute statistics for columns;

create table depts_n2 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n2 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts_n2 compute statistics for columns;

create table dependents_n2 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n2 values (10, 'Michael'), (20, 'Jane');
analyze table dependents_n2 compute statistics for columns;

create table locations_n2 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n2 values (10, 'San Francisco'), (20, 'San Diego');
analyze table locations_n2 compute statistics for columns;

alter table emps_n3 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n2 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n2 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n2 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n3 add constraint fk1 foreign key (deptno) references depts_n2(deptno) disable novalidate rely;
alter table depts_n2 add constraint fk2 foreign key (locationid) references locations_n2(locationid) disable novalidate rely;

-- EXAMPLE 1
create materialized view mv1_n2 as
select * from emps_n3 where empid < 150;
analyze table mv1_n2 compute statistics for columns;

explain
select *
from (select * from emps_n3 where empid < 120) t
join depts_n2 using (deptno);

select *
from (select * from emps_n3 where empid < 120) t
join depts_n2 using (deptno);

drop materialized view mv1_n2;

-- EXAMPLE 2
create materialized view mv1_n2 as
select deptno, name, salary, commission
from emps_n3;
analyze table mv1_n2 compute statistics for columns;

explain
select emps_n3.name, emps_n3.salary, emps_n3.commission
from emps_n3
join depts_n2 using (deptno);

select emps_n3.name, emps_n3.salary, emps_n3.commission
from emps_n3
join depts_n2 using (deptno);

drop materialized view mv1_n2;

-- EXAMPLE 3
create materialized view mv1_n2 as
select empid deptno from emps_n3
join depts_n2 using (deptno);
analyze table mv1_n2 compute statistics for columns;

explain
select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 1;

select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 1;

drop materialized view mv1_n2;

-- EXAMPLE 4
create materialized view mv1_n2 as
select * from emps_n3 where empid < 200;
analyze table mv1_n2 compute statistics for columns;

explain
select * from emps_n3 where empid > 120
union all select * from emps_n3 where empid < 150;

select * from emps_n3 where empid > 120
union all select * from emps_n3 where empid < 150;

drop materialized view mv1_n2;

-- EXAMPLE 5 - NO MV, ALREADY UNIQUE
create materialized view mv1_n2 as
select empid, deptno from emps_n3 group by empid, deptno;
analyze table mv1_n2 compute statistics for columns;

explain
select empid, deptno from emps_n3 group by empid, deptno;

select empid, deptno from emps_n3 group by empid, deptno;

drop materialized view mv1_n2;

-- EXAMPLE 5 - NO MV, ALREADY UNIQUE
create materialized view mv1_n2 as
select empid, name from emps_n3 group by empid, name;
analyze table mv1_n2 compute statistics for columns;

explain
select empid, name from emps_n3 group by empid, name;

select empid, name from emps_n3 group by empid, name;

drop materialized view mv1_n2;

-- EXAMPLE 5
create materialized view mv1_n2 as
select name, salary from emps_n3 group by name, salary;
analyze table mv1_n2 compute statistics for columns;

explain
select name, salary from emps_n3 group by name, salary;

select name, salary from emps_n3 group by name, salary;

drop materialized view mv1_n2;

-- EXAMPLE 6
create materialized view mv1_n2 as
select name, salary from emps_n3 group by name, salary;
analyze table mv1_n2 compute statistics for columns;

explain
select name from emps_n3 group by name;

select name from emps_n3 group by name;

drop materialized view mv1_n2;

-- EXAMPLE 7
create materialized view mv1_n2 as
select name, salary from emps_n3 where deptno = 10 group by name, salary;
analyze table mv1_n2 compute statistics for columns;

explain
select name from emps_n3 where deptno = 10 group by name;

select name from emps_n3 where deptno = 10 group by name;

drop materialized view mv1_n2;

-- EXAMPLE 9
create materialized view mv1_n2 as
select name, salary, count(*) as c, sum(empid) as s
from emps_n3 group by name, salary;
analyze table mv1_n2 compute statistics for columns;

explain
select name from emps_n3 group by name;

select name from emps_n3 group by name;

drop materialized view mv1_n2;
