-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n9 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n9 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (120, 10, 'Theodore', 10000, 250);

create table depts_n7 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n7 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);

create table dependents_n5 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n5 values (10, 'Michael'), (20, 'Jane');

create table locations_n5 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n5 values (10, 'San Francisco'), (20, 'San Diego');

alter table emps_n9 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n7 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n5 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n5 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n9 add constraint fk1 foreign key (deptno) references depts_n7(deptno) disable novalidate rely;
alter table depts_n7 add constraint fk2 foreign key (locationid) references locations_n5(locationid) disable novalidate rely;

-- EXAMPLE 34
create materialized view mv1_n5 as
select empid deptno from emps_n9
join depts_n7 using (deptno);

explain
select empid deptno from emps_n9
join depts_n7 using (deptno) where empid = 1;

select empid deptno from emps_n9
join depts_n7 using (deptno) where empid = 1;

drop materialized view mv1_n5;

-- EXAMPLE 35
create materialized view mv1_n5 as
select cast(empid as BIGINT) from emps_n9
join depts_n7 using (deptno);

explain
select empid deptno from emps_n9
join depts_n7 using (deptno) where empid > 1;

select empid deptno from emps_n9
join depts_n7 using (deptno) where empid > 1;

drop materialized view mv1_n5;

-- EXAMPLE 36
create materialized view mv1_n5 as
select cast(empid as BIGINT) from emps_n9
join depts_n7 using (deptno);

explain
select empid deptno from emps_n9
join depts_n7 using (deptno) where empid = 1;

select empid deptno from emps_n9
join depts_n7 using (deptno) where empid = 1;

drop materialized view mv1_n5;

-- EXAMPLE 38
create materialized view mv1_n5 as
select depts_n7.name
from emps_n9
join depts_n7 on (emps_n9.deptno = depts_n7.deptno);

explain
select dependents_n5.empid
from depts_n7
join dependents_n5 on (depts_n7.name = dependents_n5.name)
join emps_n9 on (emps_n9.deptno = depts_n7.deptno);

select dependents_n5.empid
from depts_n7
join dependents_n5 on (depts_n7.name = dependents_n5.name)
join emps_n9 on (emps_n9.deptno = depts_n7.deptno);

drop materialized view mv1_n5;

