SET hive.cli.errors.ignore=true;
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET metastore.strict.managed.tables=true;
SET hive.default.fileformat=textfile;
SET hive.default.fileformat.managed=orc;
SET metastore.create.as.acid=true;

drop database if exists count_distinct cascade;
create database count_distinct;
use count_distinct;

create table base (c1 int, c2 int);
insert into base values (1,2),(1,1),(3,null),(2,2),(2,1),(null,3),(null,null);

explain cbo
select count(distinct c1) from base group by c2 ;
select count(distinct c1) from base group by c2 ;

create materialized view base_mview1 stored as orc as select distinct c1 c1, c2 c2 from base;

explain cbo
select count(distinct c1) from base group by c2 ;
select count(distinct c1) from base group by c2 ;

drop materialized view base_mview1;

create materialized view base_mview2 stored as orc as SELECT c1 c1, c2 c2, sum(c2) FROM base group by c1, c2;

explain cbo
select count(distinct c1) from base group by c2 ;
select count(distinct c1) from base group by c2 ;

drop materialized view base_mview2;

drop table base;
