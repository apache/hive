-- Acid table creation is not allowed when EXTERNAL_TABLES_ONLY property is set on database.
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=false;
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

create database repl_db_test with DBPROPERTIES('EXTERNAL_TABLES_ONLY'='true');
use repl_db_test;

CREATE TRANSACTIONAL TABLE transactional_table_test(key string, value string);