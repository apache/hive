-- Materialized view with runtime constant function can not be used in automatic query rewrites

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(a int, week_end_year int) stored as orc TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
SELECT a, week_end_year FROM t1 WHERE week_end_year = year(from_unixtime( unix_timestamp() ));

create materialized view mat2 as
SELECT a, week_end_year FROM t1 WHERE week_end_year = year( current_timestamp() );

describe formatted mat1;
describe formatted mat2;

show materialized views;
