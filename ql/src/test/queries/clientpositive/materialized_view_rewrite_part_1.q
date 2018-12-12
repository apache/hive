-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n30 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n30 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (120, 10, 'Bill', 10000, 250);

create table depts_n20 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n20 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);

create table dependents_n20 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n20 values (10, 'Michael'), (20, 'Jane');

create table locations_n20 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n20 values (10, 'San Francisco'), (20, 'San Diego');

alter table emps_n30 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n20 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n20 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n20 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n30 add constraint fk1 foreign key (deptno) references depts_n20(deptno) disable novalidate rely;
alter table depts_n20 add constraint fk2 foreign key (locationid) references locations_n20(locationid) disable novalidate rely;

-- EXAMPLE 1
create materialized view mv1_part_n2 partitioned on (deptno) as
select * from emps_n30 where empid < 150;

explain
select *
from (select * from emps_n30 where empid < 120) t
join depts_n20 using (deptno);

select *
from (select * from emps_n30 where empid < 120) t
join depts_n20 using (deptno);

drop materialized view mv1_part_n2;

-- EXAMPLE 2
create materialized view mv1_part_n2 partitioned on (deptno) as
select deptno, name, salary, commission
from emps_n30;

explain
select emps_n30.name, emps_n30.salary, emps_n30.commission
from emps_n30
join depts_n20 using (deptno);

select emps_n30.name, emps_n30.salary, emps_n30.commission
from emps_n30
join depts_n20 using (deptno);

drop materialized view mv1_part_n2;

-- EXAMPLE 4
create materialized view mv1_part_n2 partitioned on (deptno) as
select * from emps_n30 where empid < 200;

explain
select * from emps_n30 where empid > 120
union all select * from emps_n30 where empid < 150;

select * from emps_n30 where empid > 120
union all select * from emps_n30 where empid < 150;

drop materialized view mv1_part_n2;

-- EXAMPLE 5
create materialized view mv1_part_n2 partitioned on (name) as
select name, salary from emps_n30 group by name, salary;

explain
select name, salary from emps_n30 group by name, salary;

select name, salary from emps_n30 group by name, salary;

drop materialized view mv1_part_n2;

-- EXAMPLE 6
create materialized view mv1_part_n2 partitioned on (name) as
select name, salary from emps_n30 group by name, salary;

explain
select name from emps_n30 group by name;

select name from emps_n30 group by name;

drop materialized view mv1_part_n2;

-- EXAMPLE 7
create materialized view mv1_part_n2 partitioned on (name) as
select name, salary from emps_n30 where deptno = 10 group by name, salary;

explain
select name from emps_n30 where deptno = 10 group by name;

select name from emps_n30 where deptno = 10 group by name;

drop materialized view mv1_part_n2;

-- EXAMPLE 9
create materialized view mv1_part_n2 partitioned on (name) as
select name, salary, count(*) as c, sum(empid) as s
from emps_n30 group by name, salary;

explain
select name from emps_n30 group by name;

select name from emps_n30 group by name;

drop materialized view mv1_part_n2;
