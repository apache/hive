create table if not exists alltypes (
 bo boolean,
 ti tinyint,
 si smallint,
 i int,
 bi bigint,
 f float,
 d double,
 de decimal(10,3),
 ts timestamp,
 da date,
 s string,
 c char(5),
 vc varchar(5),
 m map<string, string>,
 l array<int>,
 st struct<c1:int, c2:string>
) row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;

create table alltypes_orc like alltypes;
alter table alltypes_orc set fileformat orc;

load data local inpath '../../data/files/alltypes2.txt' overwrite into table alltypes;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

insert overwrite table alltypes_orc select * from alltypes;
insert into table alltypes_orc select * from alltypes;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/alltypes_orc/;

set hive.merge.orcfile.stripe.level=true;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

alter table alltypes_orc concatenate;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/alltypes_orc/;
