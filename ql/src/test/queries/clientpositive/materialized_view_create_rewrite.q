-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

create table cmv_basetable_n10 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n10 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

create materialized view cmv_mat_view_n10
as select a, b, c from cmv_basetable_n10 where a = 2;

select * from cmv_mat_view_n10;

show tblproperties cmv_mat_view_n10;

create materialized view if not exists cmv_mat_view2_n4
as select a, c from cmv_basetable_n10 where a = 3;

select * from cmv_mat_view2_n4;

show tblproperties cmv_mat_view2_n4;

explain
select a, c from cmv_basetable_n10 where a = 3;

select a, c from cmv_basetable_n10 where a = 3;

alter materialized view cmv_mat_view2_n4 disable rewrite;

explain
select * from (
  (select a, c from cmv_basetable_n10 where a = 3) table1
  join
  (select a, c from cmv_basetable_n10 where d = 3) table2
  on table1.a = table2.a);

select * from (
  (select a, c from cmv_basetable_n10 where a = 3) table1
  join
  (select a, c from cmv_basetable_n10 where d = 3) table2
  on table1.a = table2.a);

explain
alter materialized view cmv_mat_view2_n4 enable rewrite;

alter materialized view cmv_mat_view2_n4 enable rewrite;

explain
select * from (
  (select a, c from cmv_basetable_n10 where a = 3) table1
  join
  (select a, c from cmv_basetable_n10 where d = 3) table2
  on table1.a = table2.a);

select * from (
  (select a, c from cmv_basetable_n10 where a = 3) table1
  join
  (select a, c from cmv_basetable_n10 where d = 3) table2
  on table1.a = table2.a);

drop materialized view cmv_mat_view2_n4;

explain
select * from (
  (select a, c from cmv_basetable_n10 where a = 3) table1
  join
  (select a, c from cmv_basetable_n10 where d = 3) table2
  on table1.a = table2.a);

select * from (
  (select a, c from cmv_basetable_n10 where a = 3) table1
  join
  (select a, c from cmv_basetable_n10 where d = 3) table2
  on table1.a = table2.a);

drop materialized view cmv_mat_view_n10;
