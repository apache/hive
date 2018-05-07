--! qt:dataset:part

set hive.vectorized.execution.enabled=false;
drop table aa;
create table aa (a string, b int) stored as orc;
insert into table aa values("b",2);
insert into table aa values("c",3);

-- SORT_QUERY_RESULTS

dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/aa/000000_0 ${hiveconf:hive.metastore.warehouse.dir}/aa/part-00000;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/aa/000000_0_copy_1 ${hiveconf:hive.metastore.warehouse.dir}/aa/part-00000_copy_1;

select * from aa;

alter table aa add columns(aa string, bb int);

insert into table aa values("b",2,"b",2);
insert into table aa values("c",3,"c",3);

dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/aa/000000_0 ${hiveconf:hive.metastore.warehouse.dir}/aa/part-00001;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/aa/000000_0_copy_1 ${hiveconf:hive.metastore.warehouse.dir}/aa/part-00001_copy_1;

select * from aa;
select count(*) from aa;
select sum(hash(*)) from aa;

-- try concatenate multiple times (order of files chosen for concatenation is not guaranteed)
alter table aa concatenate;
select * from aa;
select count(*) from aa;
select sum(hash(*)) from aa;

alter table aa concatenate;
select * from aa;
select count(*) from aa;
select sum(hash(*)) from aa;

alter table aa concatenate;
select * from aa;
select count(*) from aa;
select sum(hash(*)) from aa;

alter table aa concatenate;
select * from aa;
select count(*) from aa;
select sum(hash(*)) from aa;
