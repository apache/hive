--! qt:dataset:src
set hive.vectorized.execution.enabled=false;
create table cmv_basetable_n4 (a int, b varchar(256), c decimal(10,2));

insert into cmv_basetable_n4 values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view cmv_mat_view_n4 disable rewrite as select a, b, c from cmv_basetable_n4;

desc formatted cmv_mat_view_n4;

select * from cmv_mat_view_n4;

create materialized view if not exists cmv_mat_view2_n1 disable rewrite as select a, c from cmv_basetable_n4;

desc formatted cmv_mat_view2_n1;

select * from cmv_mat_view2_n1;

create materialized view if not exists cmv_mat_view3 disable rewrite as select * from cmv_basetable_n4 where a > 1;

select * from cmv_mat_view3;

create materialized view cmv_mat_view4 disable rewrite comment 'this is a comment' as select a, sum(c) from cmv_basetable_n4 group by a;

select * from cmv_mat_view4;

describe extended cmv_mat_view4;

create table cmv_basetable2 (d int, e varchar(256), f decimal(10,2));

insert into cmv_basetable2 values (4, 'alfred', 100.30),(4, 'bob', 6133.14),(5, 'bonnie', 172.2),(6, 'calvin', 8.76),(17, 'charlie', 13144339.8);

create materialized view cmv_mat_view5 disable rewrite tblproperties ('key'='value') as select a, b, d, c, f from cmv_basetable_n4 t1 join cmv_basetable2 t2 on (t1.b = t2.e);

select * from cmv_mat_view5;

show tblproperties cmv_mat_view5;

drop materialized view cmv_mat_view_n4;
drop materialized view cmv_mat_view2_n1;
drop materialized view cmv_mat_view3;
drop materialized view cmv_mat_view4;
drop materialized view cmv_mat_view5;

-- ACID CMV
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.autogather=false;

create materialized view acid_cmv_part disable rewrite partitioned on (k)
  stored as orc TBLPROPERTIES ('transactional'='true')
  as select key k, value from src order by k limit 5;
select k, value from acid_cmv_part;

explain formatted
select k, value from acid_cmv_part;
