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

-- EXAMPLE 16
create materialized view mv1 enable rewrite as
select empid, depts.deptno from emps
join depts using (deptno) where depts.deptno > 10
group by empid, depts.deptno;
analyze table mv1 compute statistics for columns;

explain
select empid from emps
join depts using (deptno) where depts.deptno > 20
group by empid, depts.deptno;

select empid from emps
join depts using (deptno) where depts.deptno > 20
group by empid, depts.deptno;

drop materialized view mv1;

-- EXAMPLE 17
create materialized view mv1 enable rewrite as
select depts.deptno, empid from depts
join emps using (deptno) where depts.deptno > 10
group by empid, depts.deptno;
analyze table mv1 compute statistics for columns;

explain
select empid from emps
join depts using (deptno) where depts.deptno > 20
group by empid, depts.deptno;

select empid from emps
join depts using (deptno) where depts.deptno > 20
group by empid, depts.deptno;

drop materialized view mv1;

-- EXAMPLE 18
create materialized view mv1 enable rewrite as
select empid, depts.deptno from emps
join depts using (deptno) where emps.deptno > 10
group by empid, depts.deptno;
analyze table mv1 compute statistics for columns;

explain
select empid from emps
join depts using (deptno) where depts.deptno > 20
group by empid, depts.deptno;

select empid from emps
join depts using (deptno) where depts.deptno > 20
group by empid, depts.deptno;

drop materialized view mv1;

-- EXAMPLE 19
create materialized view mv1 enable rewrite as
select depts.deptno, emps.empid from depts
join emps using (deptno) where emps.empid > 10
group by depts.deptno, emps.empid;
analyze table mv1 compute statistics for columns;

explain
select depts.deptno from depts
join emps using (deptno) where emps.empid > 15
group by depts.deptno, emps.empid;

select depts.deptno from depts
join emps using (deptno) where emps.empid > 15
group by depts.deptno, emps.empid;

drop materialized view mv1;

-- EXAMPLE 20
create materialized view mv1 enable rewrite as
select depts.deptno, emps.empid from depts
join emps using (deptno) where emps.empid > 10
group by depts.deptno, emps.empid;
analyze table mv1 compute statistics for columns;

explain
select depts.deptno from depts
join emps using (deptno) where emps.empid > 15
group by depts.deptno;

select depts.deptno from depts
join emps using (deptno) where emps.empid > 15
group by depts.deptno;

drop materialized view mv1;

-- EXAMPLE 23
create materialized view mv1 enable rewrite as
select depts.name, dependents.name as name2, emps.deptno, depts.deptno as deptno2, dependents.empid
from depts, dependents, emps
where depts.deptno > 10
group by depts.name, dependents.name, emps.deptno, depts.deptno, dependents.empid;
analyze table mv1 compute statistics for columns;

explain
select dependents.empid
from depts
join dependents on (depts.name = dependents.name)
join emps on (emps.deptno = depts.deptno)
where depts.deptno > 10
group by dependents.empid;

select dependents.empid
from depts
join dependents on (depts.name = dependents.name)
join emps on (emps.deptno = depts.deptno)
where depts.deptno > 10
group by dependents.empid;

drop materialized view mv1;
