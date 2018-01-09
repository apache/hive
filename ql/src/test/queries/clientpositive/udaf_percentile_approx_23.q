set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- 0.23 changed input order of data in reducer task, which affects result of percentile_approx

CREATE TABLE bucket (key double, value string) CLUSTERED BY (key) SORTED BY (key DESC)  INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket;
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket;
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket;
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket;

create table t1 (result double);
create table t2 (result double);
create table t3 (result double);
create table t4 (result double);
create table t5 (result double);
create table t6 (result double);
create table t7 (result array<double>);
create table t8 (result array<double>);
create table t9 (result array<double>);
create table t10 (result array<double>);
create table t11 (result array<double>);
create table t12 (result array<double>);

set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.map.aggr=false;
-- disable map-side aggregation
FROM bucket
insert overwrite table t1 SELECT percentile_approx(cast(key AS double), 0.5)
insert overwrite table t2 SELECT percentile_approx(cast(key AS double), 0.5, 100)
insert overwrite table t3 SELECT percentile_approx(cast(key AS double), 0.5, 1000)

insert overwrite table t4 SELECT percentile_approx(cast(key AS int), 0.5)
insert overwrite table t5 SELECT percentile_approx(cast(key AS int), 0.5, 100)
insert overwrite table t6 SELECT percentile_approx(cast(key AS int), 0.5, 1000)

insert overwrite table t7 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98))
insert overwrite table t8 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t9 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 1000)

insert overwrite table t10 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98))
insert overwrite table t11 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t12 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 1000);

select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
select * from t6;
select * from t7;
select * from t8;
select * from t9;
select * from t10;
select * from t11;
select * from t12;

set hive.map.aggr=true;
-- enable map-side aggregation
FROM bucket
insert overwrite table t1 SELECT percentile_approx(cast(key AS double), 0.5)
insert overwrite table t2 SELECT percentile_approx(cast(key AS double), 0.5, 100)
insert overwrite table t3 SELECT percentile_approx(cast(key AS double), 0.5, 1000)

insert overwrite table t4 SELECT percentile_approx(cast(key AS int), 0.5)
insert overwrite table t5 SELECT percentile_approx(cast(key AS int), 0.5, 100)
insert overwrite table t6 SELECT percentile_approx(cast(key AS int), 0.5, 1000)

insert overwrite table t7 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98))
insert overwrite table t8 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t9 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 1000)

insert overwrite table t10 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98))
insert overwrite table t11 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t12 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 1000);

select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
select * from t6;
select * from t7;
select * from t8;
select * from t9;
select * from t10;
select * from t11;
select * from t12;

-- NaN
explain
select percentile_approx(case when key < 100 then cast('NaN' as double) else key end, 0.5) from bucket;
select percentile_approx(case when key < 100 then cast('NaN' as double) else key end, 0.5) between 340.5 and 343.0 from bucket;

-- with CBO
explain
select percentile_approx(key, 0.5) from bucket;
select percentile_approx(key, 0.5) between 255.0 and 257.0 from bucket;

-- test where number of elements is zero
set hive.cbo.enable=false;
select percentile_approx(key, array(0.50, 0.70, 0.90, 0.95, 0.99)) from bucket where key > 10000;
