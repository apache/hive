--! qt:dataset:alltypesorc,alltypesparquet,cbo_t1,cbo_t2,cbo_t3,lineitem,part,src,src1,src_cbo,src_json,src_sequencefile,src_thrift,srcbucket,srcbucket2,srcpart
--! qt:sysdb

set hive.strict.checks.cartesian.product=false;

set hive.compute.query.using.stats=false;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.cbo.enable=false;
set metastore.strict.managed.tables=true;


create external table smt_sysdb_src_buck (key int, value string) clustered by(value) into 2 buckets;

create external table smt_sysdb_src_skew (key int) skewed by (key) on (1,2,3);

CREATE TABLE smt_sysdb_scr_txn (key int, value string)
    CLUSTERED BY (key) INTO 2 BUCKETS STORED AS ORC
    TBLPROPERTIES (
      "transactional"="true",
      "compactor.mapreduce.map.memory.mb"="2048",
      "compactorthreshold.hive.compactor.delta.num.threshold"="4",
      "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5");

CREATE TEMPORARY TABLE smt_sysdb_src_tmp (key int, value string);

CREATE EXTERNAL TABLE smt_sysdb_moretypes (a decimal(10,2), b tinyint, c smallint, d int, e bigint, f varchar(10), g char(3));

CREATE VIEW smt_sysdb_view
    AS select smt_sysdb_src_buck.key, smt_sysdb_scr_txn.value
       from smt_sysdb_src_buck, smt_sysdb_scr_txn where smt_sysdb_src_buck.key = smt_sysdb_scr_txn.key;

show grant user hive_test_user;

use sys;

select tbl_name, tbl_type from tbls where tbl_name like 'smt_sysdb%' order by tbl_name;

drop table smt_sysdb_src_buck;
drop table smt_sysdb_src_skew;
drop table smt_sysdb_scr_txn;
drop table smt_sysdb_src_tmp;
drop table smt_sysdb_moretypes;

DROP DATABASE IF EXISTS SYS CASCADE;
DROP DATABASE IF EXISTS INFORMATION_SCHEMA CASCADE;
