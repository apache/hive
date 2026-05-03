-- Test subsequent rebuild of transactional MV when source table is Iceberg.

-- SORT_QUERY_RESULTS
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;

create table ice_basetable (a int, b string) stored by iceberg;
insert into ice_basetable values (1, 'alfred'),(2, 'bob'),(2, 'bonnie'),(3, 'calvin'),(3, 'charlie');

create materialized view mv_acid  STORED AS ORC TBLPROPERTIES ('transactional'='true') as
select b, a from ice_basetable;

create materialized view mv_insert_only STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select b, a from ice_basetable;

select * from mv_acid;
select * from mv_insert_only;

insert into ice_basetable values (5, 'amia');
alter materialized view mv_acid rebuild;
alter materialized view mv_insert_only rebuild;

insert into ice_basetable values (4, 'mania');
alter materialized view mv_acid rebuild;
alter materialized view mv_insert_only rebuild;

select * from mv_acid;
select * from mv_insert_only;
