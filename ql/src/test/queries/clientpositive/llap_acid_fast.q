set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=true;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.fetch.task.conversion=none;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE orc_llap_acid_fast;

CREATE TABLE orc_llap_acid_fast (
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE)
partitioned by (csmallint smallint)
clustered by (cint) into 2 buckets stored as orc
TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default');

insert into table orc_llap_acid_fast partition (csmallint = 1)
select cint, cbigint, cfloat, cdouble from alltypesorc order by cdouble asc limit 10;
insert into table orc_llap_acid_fast partition (csmallint = 2)
select cint, cbigint, cfloat, cdouble from alltypesorc order by cdouble asc limit 10;
insert into table orc_llap_acid_fast partition (csmallint = 3)
select cint, cbigint, cfloat, cdouble from alltypesorc order by cdouble desc limit 10;

explain vectorization only detail
select cint, csmallint, cbigint from orc_llap_acid_fast where cint is not null order
by csmallint, cint;
select cint, csmallint, cbigint from orc_llap_acid_fast where cint is not null order
by csmallint, cint;

insert into table orc_llap_acid_fast partition (csmallint = 1) values (1, 1, 1, 1);

explain vectorization only detail
update orc_llap_acid_fast set cbigint = 2 where cint = 1;
update orc_llap_acid_fast set cbigint = 2 where cint = 1;

explain vectorization only detail
select cint, csmallint, cbigint from orc_llap_acid_fast where cint is not null order
by csmallint, cint;
select cint, csmallint, cbigint from orc_llap_acid_fast where cint is not null order
by csmallint, cint;

DROP TABLE orc_llap_acid_fast;
