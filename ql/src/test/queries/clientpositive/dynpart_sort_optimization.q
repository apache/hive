set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=false;
set hive.enforce.sorting=false;

create table over1k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over1k' into table over1k;

create table over1k_part(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (ds string, t tinyint);

create table over1k_part_limit like over1k_part;

create table over1k_part_buck(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) into 4 buckets;

create table over1k_part_buck_sort(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) 
       sorted by (f) into 4 buckets;

-- map-only jobs converted to map-reduce job by hive.optimize.sort.dynamic.partition optimization
explain insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27;
explain insert overwrite table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27 limit 10;
explain insert overwrite table over1k_part_buck partition(t) select si,i,b,f,t from over1k where t is null or t=27;
explain insert overwrite table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k where t is null or t=27;

insert overwrite table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27;
insert overwrite table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27 limit 10;
insert overwrite table over1k_part_buck partition(t) select si,i,b,f,t from over1k where t is null or t=27;
insert overwrite table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k where t is null or t=27;

set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;

-- map-reduce jobs modified by hive.optimize.sort.dynamic.partition optimization
explain insert into table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27;
explain insert into table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27 limit 10;
explain insert into table over1k_part_buck partition(t) select si,i,b,f,t from over1k where t is null or t=27;
explain insert into table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k where t is null or t=27;

insert into table over1k_part partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27;
insert into table over1k_part_limit partition(ds="foo", t) select si,i,b,f,t from over1k where t is null or t=27 limit 10;
insert into table over1k_part_buck partition(t) select si,i,b,f,t from over1k where t is null or t=27;
insert into table over1k_part_buck_sort partition(t) select si,i,b,f,t from over1k where t is null or t=27;

desc formatted over1k_part partition(ds="foo",t=27);
desc formatted over1k_part partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_limit partition(ds="foo",t=27);
desc formatted over1k_part_limit partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck partition(t=27);
desc formatted over1k_part_buck partition(t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck_sort partition(t=27);
desc formatted over1k_part_buck_sort partition(t="__HIVE_DEFAULT_PARTITION__");

select count(*) from over1k_part;
select count(*) from over1k_part_limit;
select count(*) from over1k_part_buck;
select count(*) from over1k_part_buck_sort;
