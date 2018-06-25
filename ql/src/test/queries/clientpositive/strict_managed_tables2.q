
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set metastore.strict.managed.tables=true;
set hive.default.fileformat=textfile;
set hive.default.fileformat.managed=orc;

set metastore.create.as.acid=true;
set hive.create.as.insert.only=false;

drop table if exists smt2_tab1;
drop table if exists smt2_tab2;
drop table if exists smt2_tab3;
drop table if exists smt2_tab4;
drop table if exists smt2_tab5;
drop table if exists smt2_tab6;

create external table smt2_tab1 (c1 string, c2 string);
show create table smt2_tab1;

create table smt2_tab2 (c1 string, c2 string);
show create table smt2_tab2;

create table smt2_tab3 (c1 string, c2 string) stored as orc;
show create table smt2_tab3;

set metastore.create.as.acid=false;
set hive.create.as.insert.only=true;

create external table smt2_tab4 (c1 string, c2 string) stored as orc;
show create table smt2_tab4;

create table smt2_tab5 (c1 string, c2 string);
show create table smt2_tab5;

create table smt2_tab6 (c1 string, c2 string) stored as textfile;
show create table smt2_tab6;

drop table if exists smt2_tab1;
drop table if exists smt2_tab2;
drop table if exists smt2_tab3;
drop table if exists smt2_tab4;
drop table if exists smt2_tab5;
drop table if exists smt2_tab6;

