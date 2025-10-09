set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists tbl_acid;
create table tbl_acid(a int, b string, c int) stored as orc TBLPROPERTIES ('transactional'='true');

drop table if exists tbl_ice;
create external table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');

create materialized view mat1 as
select tbl_acid.b, tbl_acid.c from tbl_ice join tbl_acid on tbl_ice.a = tbl_acid.a;
