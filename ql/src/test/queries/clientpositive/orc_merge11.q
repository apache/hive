DROP TABLE orcfile_merge1;
DROP TABLE orc_split_elim;

create table orc_split_elim (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_split_elim;
load data local inpath '../../data/files/orc_split_elim.orc' into table orc_split_elim;

create table orcfile_merge1 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc tblproperties("orc.compress.size"="4096");

insert overwrite table orcfile_merge1 select * from orc_split_elim;
insert into table orcfile_merge1 select * from orc_split_elim;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1/;

set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.orcfile.stripe.level=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set tez.am.grouping.split-count=1;
set tez.grouping.split-count=1;
set hive.exec.orc.default.buffer.size=120;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
select * from orcfile_merge1 limit 1;
SET hive.exec.post.hooks=;

-- concatenate
ALTER TABLE  orcfile_merge1 CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1/;

select count(*) from orc_split_elim;
-- will have double the number of rows
select count(*) from orcfile_merge1;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
select * from orcfile_merge1 limit 1;
SET hive.exec.post.hooks=;

DROP TABLE orc_split_elim;
DROP TABLE orcfile_merge1;
