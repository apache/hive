set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- 0.23 changed input order of data in reducer task, which affects result of percentile_approx

CREATE TABLE bucket_n0 (key double, value string) CLUSTERED BY (key) SORTED BY (key DESC)  INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_n0;
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_n0;
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_n0;
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_n0;

create table t1_n58 (result double);
create table t2_n36 (result double);
create table t3_n13 (result double);
create table t4_n6 (result double);
create table t5_n2 (result double);
create table t6_n1 (result double);
create table t7_n3 (result array<double>);
create table t8_n1 (result array<double>);
create table t9_n0 (result array<double>);
create table t10_n0 (result array<double>);
create table t11_n0 (result array<double>);
create table t12_n0 (result array<double>);

set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.map.aggr=false;
-- disable map-side aggregation
FROM bucket_n0
insert overwrite table t1_n58 SELECT percentile_approx(cast(key AS double), 0.5)
insert overwrite table t2_n36 SELECT percentile_approx(cast(key AS double), 0.5, 100)
insert overwrite table t3_n13 SELECT percentile_approx(cast(key AS double), 0.5, 1000)

insert overwrite table t4_n6 SELECT percentile_approx(cast(key AS int), 0.5)
insert overwrite table t5_n2 SELECT percentile_approx(cast(key AS int), 0.5, 100)
insert overwrite table t6_n1 SELECT percentile_approx(cast(key AS int), 0.5, 1000)

insert overwrite table t7_n3 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98))
insert overwrite table t8_n1 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t9_n0 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 1000)

insert overwrite table t10_n0 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98))
insert overwrite table t11_n0 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t12_n0 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 1000);

select * from t1_n58;
select * from t2_n36;
select * from t3_n13;
select * from t4_n6;
select * from t5_n2;
select * from t6_n1;
select * from t7_n3;
select * from t8_n1;
select * from t9_n0;
select * from t10_n0;
select * from t11_n0;
select * from t12_n0;

set hive.map.aggr=true;
-- enable map-side aggregation
FROM bucket_n0
insert overwrite table t1_n58 SELECT percentile_approx(cast(key AS double), 0.5)
insert overwrite table t2_n36 SELECT percentile_approx(cast(key AS double), 0.5, 100)
insert overwrite table t3_n13 SELECT percentile_approx(cast(key AS double), 0.5, 1000)

insert overwrite table t4_n6 SELECT percentile_approx(cast(key AS int), 0.5)
insert overwrite table t5_n2 SELECT percentile_approx(cast(key AS int), 0.5, 100)
insert overwrite table t6_n1 SELECT percentile_approx(cast(key AS int), 0.5, 1000)

insert overwrite table t7_n3 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98))
insert overwrite table t8_n1 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t9_n0 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 1000)

insert overwrite table t10_n0 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98))
insert overwrite table t11_n0 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t12_n0 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 1000);

select * from t1_n58;
select * from t2_n36;
select * from t3_n13;
select * from t4_n6;
select * from t5_n2;
select * from t6_n1;
select * from t7_n3;
select * from t8_n1;
select * from t9_n0;
select * from t10_n0;
select * from t11_n0;
select * from t12_n0;

-- NaN
explain
select percentile_approx(case when key < 100 then cast('NaN' as double) else key end, 0.5) from bucket_n0;
select percentile_approx(case when key < 100 then cast('NaN' as double) else key end, 0.5) between 340.5 and 343.0 from bucket_n0;

-- with CBO
explain
select percentile_approx(key, 0.5) from bucket_n0;
select percentile_approx(key, 0.5) between 255.0 and 257.0 from bucket_n0;

-- test where number of elements is zero
select percentile_approx(key, array(0.50, 0.70, 0.90, 0.95, 0.99)) from bucket_n0 where key > 10000;
