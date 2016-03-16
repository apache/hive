create table concat_incompat like alltypesorc;

load data local inpath '../../data/files/alltypesorc' into table concat_incompat;
load data local inpath '../../data/files/alltypesorc' into table concat_incompat;
load data local inpath '../../data/files/alltypesorc' into table concat_incompat;
load data local inpath '../../data/files/alltypesorc' into table concat_incompat;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/concat_incompat/;
select count(*) from concat_incompat;

ALTER TABLE concat_incompat CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/concat_incompat/;
select count(*) from concat_incompat;
