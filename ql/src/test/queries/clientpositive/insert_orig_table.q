set hive.strict.checks.bucketing=false;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

create table acid_iot(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN) clustered by (cint) into 1 buckets stored as orc TBLPROPERTIES ('transactional'='true');

LOAD DATA LOCAL INPATH "../../data/files/alltypesorc" into table acid_iot;

select count(*) from acid_iot;

insert into table acid_iot select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2,
       cboolean1, cboolean2 from alltypesorc where cint < 0 order by cint limit 10;

select count(*) from acid_iot;

