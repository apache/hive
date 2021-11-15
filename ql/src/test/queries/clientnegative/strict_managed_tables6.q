set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.managed.tables=true;

drop database if exists smt6;
create database smt6 with dbproperties ('repl.source.for'='1,2,3');
use smt6;
create table strict_managed_tables1_tab1 (c1 string, c2 string) stored as orc tblproperties ('transactional'='true');
-- changing location of a managed table in a database which is source of replication is not allowed
alter table strict_managed_tables1_tab1 set location '/tmp/new_smt1_tab';
