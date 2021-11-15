SET hive.cli.errors.ignore=true;
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET metastore.strict.managed.tables=true;
SET hive.default.fileformat=textfile;
SET hive.default.fileformat.managed=orc;
SET metastore.create.as.acid=true;
SET hive.groupby.position.alias=true;

drop database if exists expr2 cascade;
create database expr2;
use expr2;
create table sales(prod_id int, cust_id int, store_id int, sale_date timestamp, qty int, amt double, descr string);
insert into sales values
(11,1,101,'12/24/2013',1000,1234.00,'onedummytwo');

create materialized view mv1 stored as orc as (select prod_id, cust_id, store_id, sale_date, qty, amt, descr from sales where cust_id in (1,2,3,4,5));
-- SAME ORDER
explain cbo
select prod_id, cust_id  from sales where cust_id in (1,2,3,4,5);
-- DIFFERENT ORDER
explain cbo
select prod_id, cust_id  from sales where cust_id in (5,1,2,3,4);

drop materialized view mv1;

drop database if exists in_pred cascade;
create database in_pred;
use in_pred;
create table census_pop (state string, year int, population bigint);
insert into census_pop values("AZ", 2010, 200), ("CA", 2011, 100), ("CA", 2010, 200), ("AZ", 2010, 100), ("NY", 2011, 121), ("AZ", 2011, 1000), ("OR", 2015, 1001), ("WA", 2016, 121), ("NJ", 2010, 500), ("NJ", 2010, 5000), ("AZ", 2014, 1004), ("TX", 2010, 1000), ("AZ", 2010, 1000), ("PT", 2017, 1200), ("NM", 2018, 120), ("CA", 2010, 200);

create materialized view mv2 stored as orc as select state, year, sum(population) from census_pop where year IN (2010, 2018) group by state, year;
-- SAME
explain cbo
select state, year, sum(population) from census_pop where year IN (2010, 2018) group by state, year;
-- PARTIAL IN EQUALS
explain cbo
select state, year, sum(population) from census_pop where year = 2010 group by state, year;
-- PARTIAL
explain cbo
select state, year, sum(population) from census_pop where year in (2010) group by state, year;

drop materialized view mv2;

drop database if exists expr9 cascade;
create database expr9;
use expr9;
create table sales(prod_id int, cust_id int, store_id int, sale_date timestamp, qty int, amt double, descr string);
insert into sales values
(11,1,101,'12/24/2013',1000,1234.00,'onedummytwo');

create materialized view mv3 stored as orc as (select prod_id, cust_id, store_id, sale_date, qty, amt, descr from sales where cust_id >= 1 and prod_id < 31);
-- SAME
explain cbo
select  * from sales where cust_id >= 1 and  prod_id < 31;
-- BETWEEN AND RANGE
explain cbo
select  * from sales where cust_id between 1 and 20 and prod_id < 31;

drop materialized view mv3;
