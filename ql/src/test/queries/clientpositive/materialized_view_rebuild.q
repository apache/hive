-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

drop materialized view if exists mv_rebuild;
drop table if exists basetable_rebuild;

create table basetable_rebuild (a int, b varchar(256), c decimal(10,2))
stored as orc TBLPROPERTIES ('transactional'='true');

insert into basetable_rebuild values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view mv_rebuild as select a, b, sum(a) from basetable_rebuild group by a,b;

select * from mv_rebuild;

insert into basetable_rebuild values (4, 'amia', 7.5);

select * from mv_rebuild;

alter materialized view mv_rebuild rebuild;

select * from mv_rebuild;

drop materialized view mv_rebuild;
drop table basetable_rebuild;
