set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/delete_orig_table;
dfs -copyFromLocal ../../data/files/alltypesorc ${system:test.tmp.dir}/delete_orig_table/00000_0; 

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
    cboolean2 BOOLEAN) clustered by (cint) into 1 buckets stored as orc location '${system:test.tmp.dir}/delete_orig_table' TBLPROPERTIES ('transactional'='true');

select count(*) from acid_dot;

delete from acid_dot where cint < -1070551679;

select count(*) from acid_dot;

dfs -rmr ${system:test.tmp.dir}/delete_orig_table;
