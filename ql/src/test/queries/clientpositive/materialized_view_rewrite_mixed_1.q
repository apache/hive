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

create table depts_n2 (
  deptno int,
  name varchar(256),
  locationid int)
stored as orc TBLPROPERTIES ('transactional'='false');
insert into depts_n2 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);

-- EXAMPLE 1
create materialized view mv1_n2 as
select * from emps_n3 where empid < 150;

explain cbo
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

explain cbo
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

explain cbo
select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 100;

select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 100;

drop materialized view mv1_n2;

create materialized view mv1_n2 TBLPROPERTIES ('rewriting.time.window' = '-1') as
select empid deptno from emps_n3
join depts_n2 using (deptno);

explain cbo
select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 100;

select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 100;

insert into emps_n3 values (130, 10, 'Jane', 100000, 250);

explain
alter materialized view mv1_n2 rebuild;

alter materialized view mv1_n2 rebuild;

explain cbo
select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 100;

select empid deptno from emps_n3
join depts_n2 using (deptno) where empid = 100;

drop materialized view mv1_n2;

-- EXAMPLE 4
create materialized view mv1_n2 as
select * from emps_n3 where empid < 200;

explain cbo
select * from emps_n3 where empid > 120
union all select * from emps_n3 where empid < 150;

select * from emps_n3 where empid > 120
union all select * from emps_n3 where empid < 150;

drop materialized view mv1_n2;

-- EXAMPLE 5a
create materialized view mv1_n2 as
select empid, deptno from emps_n3 group by empid, deptno;

explain cbo
select empid, deptno from emps_n3 group by empid, deptno;

select empid, deptno from emps_n3 group by empid, deptno;

drop materialized view mv1_n2;

-- EXAMPLE 5b
create materialized view mv1_n2 as
select empid, name from emps_n3 group by empid, name;

explain cbo
select empid, name from emps_n3 group by empid, name;

select empid, name from emps_n3 group by empid, name;

drop materialized view mv1_n2;

-- EXAMPLE 5c
create materialized view mv1_n2 as
select name, salary from emps_n3 group by name, salary;

explain cbo
select name, salary from emps_n3 group by name, salary;

select name, salary from emps_n3 group by name, salary;

drop materialized view mv1_n2;

-- EXAMPLE 6
create materialized view mv1_n2 as
select name, salary from emps_n3 group by name, salary;

explain cbo
select name from emps_n3 group by name;

select name from emps_n3 group by name;

drop materialized view mv1_n2;

-- EXAMPLE 7
create materialized view mv1_n2 as
select name, salary from emps_n3 where deptno = 10 group by name, salary;

explain cbo
select name from emps_n3 where deptno = 10 group by name;

select name from emps_n3 where deptno = 10 group by name;

drop materialized view mv1_n2;

-- EXAMPLE 9
create materialized view mv1_n2 as
select name, salary, count(*) as c, sum(empid) as s
from emps_n3 group by name, salary;

explain cbo
select name from emps_n3 group by name;

select name from emps_n3 group by name;

drop materialized view mv1_n2;

-- NEW 1
create materialized view mv1_n2 TBLPROPERTIES ('rewriting.time.window' = '-1') as
select deptno, name, count(*) as c
from depts_n2
group by deptno, name;

explain cbo
select name, count(*) as c
from depts_n2
where name = 'Sales'
group by name;

select name, count(*) as c
from depts_n2
where name = 'Sales'
group by name;

drop materialized view mv1_n2;

-- NEW 2
create materialized view mv1_n2 TBLPROPERTIES ('rewriting.time.window' = '-1') as
select deptno, name, locationid, count(*) as c
from depts_n2
group by deptno, name, locationid;

explain cbo
select deptno, name, count(*) as c
from depts_n2
where name = 'Sales'
group by deptno, name;

select deptno, name, count(*) as c
from depts_n2
where name = 'Sales'
group by deptno, name;

drop materialized view mv1_n2;

-- NEW 3
create materialized view mv1_n3 TBLPROPERTIES ('rewriting.time.window' = '-1') as
select count(*) as c
from depts_n2;

explain cbo
select count(*) as c
from depts_n2;

explain
select count(*) as c
from depts_n2;

select count(*) as c
from depts_n2;

drop materialized view mv1_n3;
