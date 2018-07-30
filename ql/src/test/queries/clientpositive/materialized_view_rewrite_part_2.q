-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_n00 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n00 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);
analyze table emps_n00 compute statistics for columns;

create table depts_n00 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into depts_n00 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);
analyze table depts_n00 compute statistics for columns;

create table dependents_n00 (
  empid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into dependents_n00 values (10, 'Michael'), (10, 'Jane');
analyze table dependents_n00 compute statistics for columns;

create table locations_n00 (
  locationid int,
  name varchar(256))
stored as orc TBLPROPERTIES ('transactional'='true');
insert into locations_n00 values (10, 'San Francisco'), (10, 'San Diego');
analyze table locations_n00 compute statistics for columns;

alter table emps_n00 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_n00 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_n00 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_n00 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_n00 add constraint fk1 foreign key (deptno) references depts_n00(deptno) disable novalidate rely;
alter table depts_n00 add constraint fk2 foreign key (locationid) references locations_n00(locationid) disable novalidate rely;

-- EXAMPLE 16
create materialized view mv1_part_n0 partitioned on (deptno) as
select empid, depts_n00.deptno as deptno from emps_n00
join depts_n00 using (deptno) where depts_n00.deptno > 10
group by empid, depts_n00.deptno;
analyze table mv1_part_n0 compute statistics for columns;

explain
select empid from emps_n00
join depts_n00 using (deptno) where depts_n00.deptno > 20
group by empid, depts_n00.deptno;

select empid from emps_n00
join depts_n00 using (deptno) where depts_n00.deptno > 20
group by empid, depts_n00.deptno;

drop materialized view mv1_part_n0;

-- EXAMPLE 17
create materialized view mv1_part_n0 partitioned on (deptno) as
select depts_n00.deptno as deptno, empid from depts_n00
join emps_n00 using (deptno) where depts_n00.deptno > 10
group by empid, depts_n00.deptno;
analyze table mv1_part_n0 compute statistics for columns;

explain
select empid from emps_n00
join depts_n00 using (deptno) where depts_n00.deptno > 20
group by empid, depts_n00.deptno;

select empid from emps_n00
join depts_n00 using (deptno) where depts_n00.deptno > 20
group by empid, depts_n00.deptno;

drop materialized view mv1_part_n0;

-- EXAMPLE 18
create materialized view mv1_part_n0 partitioned on (deptno) as
select empid, depts_n00.deptno as deptno from emps_n00
join depts_n00 using (deptno) where emps_n00.deptno > 10
group by empid, depts_n00.deptno;
analyze table mv1_part_n0 compute statistics for columns;

explain
select empid from emps_n00
join depts_n00 using (deptno) where depts_n00.deptno > 20
group by empid, depts_n00.deptno;

select empid from emps_n00
join depts_n00 using (deptno) where depts_n00.deptno > 20
group by empid, depts_n00.deptno;

drop materialized view mv1_part_n0;

-- EXAMPLE 19
create materialized view mv1_part_n0 partitioned on (deptno) as
select depts_n00.deptno as deptno, emps_n00.empid from depts_n00
join emps_n00 using (deptno) where emps_n00.empid > 10
group by depts_n00.deptno, emps_n00.empid;
analyze table mv1_part_n0 compute statistics for columns;

explain
select depts_n00.deptno from depts_n00
join emps_n00 using (deptno) where emps_n00.empid > 15
group by depts_n00.deptno, emps_n00.empid;

select depts_n00.deptno from depts_n00
join emps_n00 using (deptno) where emps_n00.empid > 15
group by depts_n00.deptno, emps_n00.empid;

drop materialized view mv1_part_n0;

-- EXAMPLE 20
create materialized view mv1_part_n0 partitioned on (deptno) as
select depts_n00.deptno as deptno, emps_n00.empid from depts_n00
join emps_n00 using (deptno) where emps_n00.empid > 10
group by depts_n00.deptno, emps_n00.empid;
analyze table mv1_part_n0 compute statistics for columns;

explain
select depts_n00.deptno from depts_n00
join emps_n00 using (deptno) where emps_n00.empid > 15
group by depts_n00.deptno;

select depts_n00.deptno from depts_n00
join emps_n00 using (deptno) where emps_n00.empid > 15
group by depts_n00.deptno;

drop materialized view mv1_part_n0;

-- EXAMPLE 23
create materialized view mv1_part_n0 partitioned on (deptno2) as
select depts_n00.name, dependents_n00.name as name2, emps_n00.deptno, depts_n00.deptno as deptno2, dependents_n00.empid
from depts_n00, dependents_n00, emps_n00
where depts_n00.deptno > 10
group by depts_n00.name, dependents_n00.name, emps_n00.deptno, depts_n00.deptno, dependents_n00.empid;
analyze table mv1_part_n0 compute statistics for columns;

explain
select dependents_n00.empid
from depts_n00
join dependents_n00 on (depts_n00.name = dependents_n00.name)
join emps_n00 on (emps_n00.deptno = depts_n00.deptno)
where depts_n00.deptno > 10
group by dependents_n00.empid;

select dependents_n00.empid
from depts_n00
join dependents_n00 on (depts_n00.name = dependents_n00.name)
join emps_n00 on (emps_n00.deptno = depts_n00.deptno)
where depts_n00.deptno > 10
group by dependents_n00.empid;

drop materialized view mv1_part_n0;
