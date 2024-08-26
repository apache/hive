--! qt:replace:/(File Version:)(.+)/$1#Masked#/
set hive.vectorized.execution.enabled=false;

DROP TABLE orcfile_merge1_n2;
DROP TABLE orc_split_elim_n0;

create table orc_split_elim_n0 (userid bigint, string1 string, subtype double, decimal1 decimal(38,0), ts timestamp) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_split_elim_n0;
load data local inpath '../../data/files/orc_split_elim.orc' into table orc_split_elim_n0;

create table orcfile_merge1_n2 (userid bigint, string1 string, subtype double, decimal1 decimal(38,0), ts timestamp) stored as orc tblproperties("orc.compress.size"="4096");

insert overwrite table orcfile_merge1_n2 select * from orc_split_elim_n0 order by userid;
insert into table orcfile_merge1_n2 select * from orc_split_elim_n0 order by userid;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1_n2/;

set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.orcfile.stripe.level=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set tez.am.grouping.split-count=1;
set tez.grouping.split-count=1;
set hive.exec.orc.default.buffer.size=120;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
select * from orcfile_merge1_n2 limit 1;
SET hive.exec.post.hooks=;

-- concatenate
ALTER TABLE  orcfile_merge1_n2 CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1_n2/;

select count(*) from orc_split_elim_n0;
-- will have double the number of rows
select count(*) from orcfile_merge1_n2;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
select * from orcfile_merge1_n2 limit 1;
SET hive.exec.post.hooks=;

SET mapreduce.job.reduces=2;

INSERT OVERWRITE DIRECTORY 'output' stored as orcfile select * from orc_split_elim_n0;

DROP TABLE orc_split_elim_n0;
DROP TABLE orcfile_merge1_n2;
