--! qt:authorizer
--! qt:scheduledqueryservice
--! qt:transactional
--! qt:sysdb

set user.name=hive_admin_user;
set role admin;

drop materialized view if exists mv1;
drop table if exists emps;
drop table if exists depts;

set hive.materializedview.rewriting=true;

-- create some tables
CREATE TABLE emps (
  empid INT,
  deptno INT,
  name VARCHAR(256),
  salary FLOAT,
  hire_date TIMESTAMP)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
 
CREATE TABLE depts (
  deptno INT,
  deptname VARCHAR(256),
  locationid INT)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

-- load data
insert into emps values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (120, 10, 'Bill', 10000, 250),
  (1330, 10, 'Bill', 10000, '2020-01-02');
insert into depts values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);


insert into emps values (1330, 10, 'Bill', 10000, '2020-01-02');

-- create mv
CREATE MATERIALIZED VIEW mv1 AS
  SELECT empid, deptname, hire_date FROM emps
    JOIN depts ON (emps.deptno = depts.deptno)
    WHERE hire_date >= '2016-01-01 00:00:00';

-- mv1 is used
EXPLAIN
SELECT empid, deptname FROM emps
JOIN depts ON (emps.deptno = depts.deptno)
WHERE hire_date >= '2018-01-01';

-- insert a new record
insert into emps values (1330, 10, 'Bill', 10000, '2020-01-02');

-- mv1 is NOT used
EXPLAIN
SELECT empid, deptname FROM emps
JOIN depts ON (emps.deptno = depts.deptno)
WHERE hire_date >= '2018-01-01';

-- create a schedule to rebuild mv (in the far future)
create scheduled query d cron '0 0 0 1 * ? 2030' defined as 
  alter materialized view mv1 rebuild;

set hive.support.quoted.identifiers=none;
select `(NEXT_EXECUTION|SCHEDULED_QUERY_ID)?+.+` from sys.scheduled_queries;

alter scheduled query d execute;

!sleep 30;

-- the scheduled execution will fail - because of missing TXN; but overall it works..
select state,error_message from sys.scheduled_executions;
