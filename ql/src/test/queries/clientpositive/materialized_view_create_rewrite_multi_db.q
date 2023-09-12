--! qt:replace:/(totalSize\s+)(\S+|\s+|.+)/$1#Masked#/
set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;
set hive.materializedview.rewriting.sql=false;
set hive.stats.column.autogather=true;

create database db1;
use db1;

create table cmv_basetable_n7 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n7 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

create database db2;
use db2;

create materialized view cmv_mat_view_n7
as select a, b, c from db1.cmv_basetable_n7 where a = 2;

select * from cmv_mat_view_n7;

show tblproperties cmv_mat_view_n7;

create materialized view if not exists cmv_mat_view2_n2
as select a, c from db1.cmv_basetable_n7 where a = 3;

select * from cmv_mat_view2_n2;

show tblproperties cmv_mat_view2_n2;

create database db3;
use db3;

explain
select a, c from db1.cmv_basetable_n7 where a = 3;

select a, c from db1.cmv_basetable_n7 where a = 3;
