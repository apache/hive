set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

create table cmv_basetable_n9 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n9 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

analyze table cmv_basetable_n9 compute statistics for columns;

create materialized view cmv_mat_view_n9 enable rewrite
as select b from cmv_basetable_n9 where c > 10.0 group by a, b, c;

-- CANNOT BE TRIGGERED
explain
select b from cmv_basetable_n9 where c > 20.0 group by a, b;

select b from cmv_basetable_n9 where c > 20.0 group by a, b;

create materialized view cmv_mat_view_2 enable rewrite
as select b, c from cmv_basetable_n9 where c > 10.0 group by a, b, c;

-- CANNOT BE TRIGGERED
explain
select b from cmv_basetable_n9 where c > 20.0 group by a, b;

select b from cmv_basetable_n9 where c > 20.0 group by a, b;

create materialized view cmv_mat_view_3 enable rewrite
as select a, b, c from cmv_basetable_n9 where c > 10.0 group by a, b, c;

-- CAN BE TRIGGERED
explain
select b from cmv_basetable_n9 where c > 20.0 group by a, b;

select b from cmv_basetable_n9 where c > 20.0 group by a, b;

create materialized view cmv_mat_view_4 enable rewrite
as select a, b from cmv_basetable_n9 group by a, b;

-- CAN BE TRIGGERED
explain
select b from cmv_basetable_n9 group by b;

select b from cmv_basetable_n9 group by b;

create table cmv_basetable_2_n4 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_2_n4 values
 (1, 'alfred', 10.30, 2),
 (3, 'calvin', 978.76, 3);

analyze table cmv_basetable_2_n4 compute statistics for columns;

create materialized view cmv_mat_view_5 enable rewrite
as select cmv_basetable_n9.a, cmv_basetable_2_n4.c
   from cmv_basetable_n9 join cmv_basetable_2_n4 on (cmv_basetable_n9.a = cmv_basetable_2_n4.a)
   where cmv_basetable_2_n4.c > 10.0
   group by cmv_basetable_n9.a, cmv_basetable_2_n4.c;

explain
select cmv_basetable_n9.a
from cmv_basetable_n9 join cmv_basetable_2_n4 on (cmv_basetable_n9.a = cmv_basetable_2_n4.a)
where cmv_basetable_2_n4.c > 10.10
group by cmv_basetable_n9.a, cmv_basetable_2_n4.c;

select cmv_basetable_n9.a
from cmv_basetable_n9 join cmv_basetable_2_n4 on (cmv_basetable_n9.a = cmv_basetable_2_n4.a)
where cmv_basetable_2_n4.c > 10.10
group by cmv_basetable_n9.a, cmv_basetable_2_n4.c;

explain
select cmv_basetable_n9.a
from cmv_basetable_n9 join cmv_basetable_2_n4 on (cmv_basetable_n9.a = cmv_basetable_2_n4.a)
where cmv_basetable_2_n4.c > 10.10
group by cmv_basetable_n9.a;

select cmv_basetable_n9.a
from cmv_basetable_n9 join cmv_basetable_2_n4 on (cmv_basetable_n9.a = cmv_basetable_2_n4.a)
where cmv_basetable_2_n4.c > 10.10
group by cmv_basetable_n9.a;

drop materialized view cmv_mat_view_n9;
drop materialized view cmv_mat_view_2;
drop materialized view cmv_mat_view_3;
drop materialized view cmv_mat_view_4;
drop materialized view cmv_mat_view_5;
