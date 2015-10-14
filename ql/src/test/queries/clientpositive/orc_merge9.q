create table ts_merge (
userid bigint,
string1 string,
subtype double,
decimal1 decimal(38,18),
ts timestamp
) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' overwrite into table ts_merge;
load data local inpath '../../data/files/orc_split_elim.orc' into table ts_merge;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/ts_merge/;

set hive.merge.orcfile.stripe.level=true;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.sparkfiles=true;

select count(*) from ts_merge;
alter table ts_merge concatenate;
select count(*) from ts_merge;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/ts_merge/;

-- incompatible merge test (stripe statistics missing)

create table a_merge like alltypesorc;

insert overwrite table a_merge select * from alltypesorc;
load data local inpath '../../data/files/alltypesorc' into table a_merge;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/a_merge/;

select count(*) from a_merge;
alter table a_merge concatenate;
select count(*) from a_merge;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/a_merge/;

insert into table a_merge select * from alltypesorc;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/a_merge/;

select count(*) from a_merge;
alter table a_merge concatenate;
select count(*) from a_merge;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/a_merge/;
