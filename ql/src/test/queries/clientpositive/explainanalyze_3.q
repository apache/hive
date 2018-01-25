set hive.map.aggr=false;

set hive.strict.checks.bucketing=false;

set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.metastore.filter.hook=org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
set hive.mapred.mode=nonstrict;
set hive.explain.user=true;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.llap.io.enabled=false;

explain analyze select key, value
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol;

explain analyze show tables;

explain analyze create database newDB location "/tmp/";

create database newDB location "/tmp/";

explain analyze describe database extended newDB;

describe database extended newDB;

explain analyze use newDB;

use newDB;

create table tab (name string);

explain analyze alter table tab rename to newName;

explain analyze drop table tab;

drop table tab;

explain analyze use default;

use default;

drop database newDB;

drop table src_stats;

create table src_stats as select * from src;

explain analyze analyze table src_stats compute statistics;

explain analyze analyze table src_stats compute statistics for columns;

explain analyze
CREATE TEMPORARY MACRO SIGMOID (x DOUBLE) 1.0 / (1.0 + EXP(-x));

CREATE TEMPORARY MACRO SIGMOID (x DOUBLE) 1.0 / (1.0 + EXP(-x));

explain analyze SELECT SIGMOID(2) FROM src LIMIT 1;
explain analyze DROP TEMPORARY MACRO SIGMOID;
DROP TEMPORARY MACRO SIGMOID;

explain analyze create table src_autho_test as select * from src;
create table src_autho_test as select * from src;

set hive.security.authorization.enabled=true;

explain analyze grant select on table src_autho_test to user hive_test_user;
grant select on table src_autho_test to user hive_test_user;

explain analyze show grant user hive_test_user on table src_autho_test;
explain analyze show grant user hive_test_user on table src_autho_test(key);

select key from src_autho_test order by key limit 20;

explain analyze revoke select on table src_autho_test from user hive_test_user;

explain analyze grant select(key) on table src_autho_test to user hive_test_user;

explain analyze revoke select(key) on table src_autho_test from user hive_test_user;

explain analyze 
create role sRc_roLE;

create role sRc_roLE;

explain analyze
grant role sRc_roLE to user hive_test_user;

grant role sRc_roLE to user hive_test_user;

explain analyze show role grant user hive_test_user;

explain analyze drop role sRc_roLE;
drop role sRc_roLE;

set hive.security.authorization.enabled=false;
drop table src_autho_test;

explain analyze drop view v;

explain analyze create view v as with cte as (select * from src  order by key limit 5)
select * from cte;

explain analyze with cte as (select * from src  order by key limit 5)
select * from cte;

create table orc_merge5 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_merge5;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=50000;
SET hive.optimize.index.filter=true;
set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.compute.splits.in.am=true;
set tez.grouping.min-size=1000;
set tez.grouping.max-size=50000;

set hive.merge.orcfile.stripe.level=true;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain analyze insert overwrite table orc_merge5 select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;

drop table orc_merge5;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.convert.join.bucket.mapjoin.tez = true;
explain analyze
select a.key, a.value, b.value
from tab a join tab_part b on a.key = b.key;



