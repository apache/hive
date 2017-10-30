set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.fetch.task.conversion=none;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE orc_llap;

CREATE TABLE orc_llap (
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE)
partitioned by (csmallint smallint)
clustered by (cint) into 2 buckets stored as orc;

insert into table orc_llap partition (csmallint = 1)
select cint, cbigint, cfloat, cdouble from alltypesorc order by cdouble asc limit 10;
insert into table orc_llap partition (csmallint = 2)
select cint, cbigint, cfloat, cdouble from alltypesorc order by cdouble asc limit 10;

alter table orc_llap SET TBLPROPERTIES ('transactional'='true');

insert into table orc_llap partition (csmallint = 3)
select cint, cbigint, cfloat, cdouble from alltypesorc order by cdouble desc limit 10;

SET hive.llap.io.enabled=true;

explain vectorization only detail
select cint, csmallint, cbigint from orc_llap where cint is not null order
by csmallint, cint;
select cint, csmallint, cbigint from orc_llap where cint is not null order
by csmallint, cint;

insert into table orc_llap partition (csmallint = 1) values (1, 1, 1, 1);

explain vectorization only detail
update orc_llap set cbigint = 2 where cint = 1;
update orc_llap set cbigint = 2 where cint = 1;

explain vectorization only detail
select cint, csmallint, cbigint from orc_llap where cint is not null order
by csmallint, cint;
select cint, csmallint, cbigint from orc_llap where cint is not null order
by csmallint, cint;

DROP TABLE orc_llap;
