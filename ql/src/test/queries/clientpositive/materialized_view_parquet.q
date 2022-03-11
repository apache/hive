-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

set hive.materializedview.fileformat=parquet;
set hive.materializedview.serde=org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;

create table emps_parquet_n3 (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into emps_parquet_n3 values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (120, 10, 'Bill', 10000, 250);

create table depts_parquet_n2 (
  deptno int,
  name varchar(256),
  locationid int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into depts_parquet_n2 values (10, 'Sales', 10), (30, 'Marketing', null), (20, 'HR', 20);

create table dependents_parquet_n2 (
  empid int,
  name varchar(256))
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into dependents_parquet_n2 values (10, 'Michael'), (20, 'Jane');

create table locations_parquet_n2 (
  locationid int,
  name varchar(256))
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into locations_parquet_n2 values (10, 'San Francisco'), (20, 'San Diego');

alter table emps_parquet_n3 add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_parquet_n2 add constraint pk2 primary key (deptno) disable novalidate rely;
alter table dependents_parquet_n2 add constraint pk3 primary key (empid) disable novalidate rely;
alter table locations_parquet_n2 add constraint pk4 primary key (locationid) disable novalidate rely;

alter table emps_parquet_n3 add constraint fk1 foreign key (deptno) references depts_parquet_n2(deptno) disable novalidate rely;
alter table depts_parquet_n2 add constraint fk2 foreign key (locationid) references locations_parquet_n2(locationid) disable novalidate rely;

-- EXAMPLE 1
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select * from emps_parquet_n3 where empid < 150;

describe formatted mv1_parquet_n2;

explain cbo
select *
from (select * from emps_parquet_n3 where empid < 120) t
join depts_parquet_n2 using (deptno);

select *
from (select * from emps_parquet_n3 where empid < 120) t
join depts_parquet_n2 using (deptno);

drop materialized view mv1_parquet_n2;

-- EXAMPLE 2
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select deptno, name, salary, commission
from emps_parquet_n3;

explain cbo
select emps_parquet_n3.name, emps_parquet_n3.salary, emps_parquet_n3.commission
from emps_parquet_n3
join depts_parquet_n2 using (deptno);

select emps_parquet_n3.name, emps_parquet_n3.salary, emps_parquet_n3.commission
from emps_parquet_n3
join depts_parquet_n2 using (deptno);

drop materialized view mv1_parquet_n2;

-- EXAMPLE 3
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select empid deptno from emps_parquet_n3
join depts_parquet_n2 using (deptno);

explain cbo
select empid deptno from emps_parquet_n3
join depts_parquet_n2 using (deptno) where empid = 1;

select empid deptno from emps_parquet_n3
join depts_parquet_n2 using (deptno) where empid = 1;

drop materialized view mv1_parquet_n2;

-- EXAMPLE 4
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select * from emps_parquet_n3 where empid < 200;

explain cbo
select * from emps_parquet_n3 where empid > 120
union all select * from emps_parquet_n3 where empid < 150;

select * from emps_parquet_n3 where empid > 120
union all select * from emps_parquet_n3 where empid < 150;

drop materialized view mv1_parquet_n2;

-- EXAMPLE 5 - NO MV, ALREADY UNIQUE
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select empid, deptno from emps_parquet_n3 group by empid, deptno;

explain cbo
select empid, deptno from emps_parquet_n3 group by empid, deptno;

select empid, deptno from emps_parquet_n3 group by empid, deptno;

drop materialized view mv1_parquet_n2;

-- EXAMPLE 5 - NO MV, ALREADY UNIQUE
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select empid, name from emps_parquet_n3 group by empid, name;

explain cbo
select empid, name from emps_parquet_n3 group by empid, name;

select empid, name from emps_parquet_n3 group by empid, name;

drop materialized view mv1_parquet_n2;

-- EXAMPLE 5
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select name, salary from emps_parquet_n3 group by name, salary;

explain cbo
select name, salary from emps_parquet_n3 group by name, salary;

select name, salary from emps_parquet_n3 group by name, salary;

drop materialized view mv1_parquet_n2;

-- EXAMPLE 6
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select name, salary from emps_parquet_n3 group by name, salary;

explain cbo
select name from emps_parquet_n3 group by name;

select name from emps_parquet_n3 group by name;

drop materialized view mv1_parquet_n2;

-- EXAMPLE 7
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select name, salary from emps_parquet_n3 where deptno = 10 group by name, salary;

explain cbo
select name from emps_parquet_n3 where deptno = 10 group by name;

select name from emps_parquet_n3 where deptno = 10 group by name;

drop materialized view mv1_parquet_n2;

-- EXAMPLE 9
create materialized view mv1_parquet_n2
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select name, salary, count(*) as c, sum(empid) as s
from emps_parquet_n3 group by name, salary;

explain cbo
select name from emps_parquet_n3 group by name;

select name from emps_parquet_n3 group by name;

insert into emps_parquet_n3 values (400, 10, 'Bill', 50000, 500);

explain
alter materialized view mv1_parquet_n2 rebuild;

alter materialized view mv1_parquet_n2 rebuild;

explain cbo
select name, sum(empid) from emps_parquet_n3 group by name;

select name, sum(empid) from emps_parquet_n3 group by name;

drop materialized view mv1_parquet_n2;

select name, sum(empid) from emps_parquet_n3 group by name;
