--! qt:dataset:alltypesorc
set hive.vectorized.execution.enabled=false;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyHiveHook;
set hive.enforce.readonly = true;

create table acid_dot(
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
    cboolean2 BOOLEAN) clustered by (cint) into 1 buckets stored as orc
    TBLPROPERTIES ('transactional'='true');

UPDATE acid_dot SET cint = 1 WHERE cfloat < 0;
