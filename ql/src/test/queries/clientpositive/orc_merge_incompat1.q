set hive.explain.user=false;
-- SORT_QUERY_RESULTS

create table orc_merge5 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;
create table orc_merge5b (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_merge5;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.merge.orcfile.stripe.level=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.merge.sparkfiles=false;

-- 3 mappers
explain insert overwrite table orc_merge5b select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;
set hive.exec.orc.write.format=0.12;
insert overwrite table orc_merge5b select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;
insert into table orc_merge5b select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;
insert into table orc_merge5b select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;
set hive.exec.orc.write.format=0.11;
insert into table orc_merge5b select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;
insert into table orc_merge5b select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;
insert into table orc_merge5b select userid,string1,subtype,decimal1,ts from orc_merge5 where userid<=13;

-- 5 files total
analyze table orc_merge5b compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5b/;
select * from orc_merge5b;

set hive.merge.orcfile.stripe.level=true;
alter table orc_merge5b concatenate;

-- 3 file after merging - all 0.12 format files will be merged and 0.11 files will be left behind
analyze table orc_merge5b compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5b/;
select * from orc_merge5b;

