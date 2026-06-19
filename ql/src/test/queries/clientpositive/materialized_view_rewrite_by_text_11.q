set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

create table cmv_basetable (a int, b varchar(256), c decimal(10,2))
stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view cmv_mat_view disable rewrite as select t1.a from cmv_basetable t1 left outer join cmv_basetable t2 on (t1.a = t1.b);

alter materialized view cmv_mat_view enable rewrite;
