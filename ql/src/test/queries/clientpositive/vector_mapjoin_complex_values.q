set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=true;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

create table census(
ssn int,
name string,
city string,
email string) 
row format delimited 
fields terminated by ',';

insert into census values(100,"raj","san jose","email");

create table census_clus(
ssn int,
name string,
city string,
email string) 
clustered by (ssn) into 4 buckets  stored as orc TBLPROPERTIES ('transactional'='true');

insert into  table census_clus select *  from census;

EXPLAIN VECTORIZATION DETAIL
UPDATE census_clus SET name = 'updated name' where ssn=100 and   EXISTS (select distinct ssn from census where ssn=census_clus.ssn);

UPDATE census_clus SET name = 'updated name' where ssn=100 and   EXISTS (select distinct ssn from census where ssn=census_clus.ssn);