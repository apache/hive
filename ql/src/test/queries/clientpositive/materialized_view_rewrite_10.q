-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;

create table emps_n10 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n10 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);
analyze table emps_n10 compute statistics for columns;

-- EXAMPLE 1
create materialized view mv1_n10 as
select deptno, sum(salary), count(salary) as a
from emps_n10 group by deptno;
analyze table mv1_n10 compute statistics for columns;

explain
select deptno, avg(salary) as a
from emps_n10 group by deptno;

select deptno, avg(salary) as a
from emps_n10 group by deptno;

drop materialized view mv1_n10;

-- EXAMPLE 2
create materialized view mv1_n10 as
select salary, sum(salary), count(salary) as a
from emps_n10 group by salary;
analyze table mv1_n10 compute statistics for columns;

explain
select salary, avg(salary) as a
from emps_n10 group by salary;

select salary, avg(salary) as a
from emps_n10 group by salary;

drop materialized view mv1_n10;

-- EXAMPLE 3
create materialized view mv1_n10 as
select salary, sum(salary), count(salary) as a
from emps_n10 where salary > 0 group by salary;
analyze table mv1_n10 compute statistics for columns;

explain
select salary, avg(salary) as a
from emps_n10 where salary > 0 group by salary;

select salary, avg(salary) as a
from emps_n10 where salary > 0 group by salary;

drop materialized view mv1_n10;

-- EXAMPLE 4
create table emps_n10_2 (
  empid int,
  deptno int,
  name varchar(256),
  salary tinyint,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps_n10_2 values (100, 10, 'Bill', 1, 1000), (200, 20, 'Eric', 2, 500),
  (150, 10, 'Sebastian', 2, null), (110, 10, 'Theodore', 3, 250), (110, 10, 'Bill', 0, 250);
analyze table emps_n10_2 compute statistics for columns;

create materialized view mv1_n10 as
select salary, sum(salary), count(salary) as a
from emps_n10_2 where salary > 0 group by salary;
analyze table mv1_n10 compute statistics for columns;

explain
select avg(salary)
from emps_n10_2 where salary > 0;

select avg(salary)
from emps_n10_2 where salary > 0;

drop materialized view mv1_n10;

drop table emps_n10;
drop table emps_n10_2;