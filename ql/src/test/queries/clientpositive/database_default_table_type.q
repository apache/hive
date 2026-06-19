set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.managed.tables=true;
set hive.create.as.acid=true;
set hive.create.as.insert.only=true;
set hive.default.fileformat.managed=ORC;
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set hive.metastore.client.capabilities=HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,HIVEMANAGESTATS,HIVEMANAGEDINSERTWRITE,HIVEMANAGEDINSERTREAD;

drop database if exists db_with_default_table_type cascade;
create database db_with_default_table_type with DBPROPERTIES("defaultTableType"="EXTERNAL");

use db_with_default_table_type;

-- Create manged tables
create table transactional_table_1 (id int, name string) tblproperties ('transactional'='true');
desc formatted transactional_table_1;

create managed table managed_table_1(id int, name string);
desc formatted managed_table_1;

create transactional table transactional_table_2 (id int, name string);
desc formatted transactional_table_2;

create transactional table transactional_table_like_1 like transactional_table_1;
desc formatted transactional_table_like_1;

create transactional table transactional_table_ctas_1 as select * from transactional_table_2;
desc formatted transactional_table_ctas_1;

-- Create external tables
create table ext_table_1(id int, name string);
desc formatted ext_table_1;

create external table ext_table_2(id int, name string);
desc formatted ext_table_2;

create table ext_table_like_1 like ext_table_1;
desc formatted ext_table_like_1;

create table ext_table_ctas_1 as select * from ext_table_2;
desc formatted ext_table_ctas_1;

drop database if exists altered_db cascade;
-- create a database which creates managed table by default
create database altered_db;
use altered_db;

-- creates managed table
create table managed_table (id int, name string);
desc formatted managed_table;

alter database altered_db set DBPROPERTIES ("defaultTableType"="EXTERNAL");

create table external_table (id int, name string);
desc formatted external_table;
