set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.cbo.returnpath.hiveop=true;
-- 0.23 changed input order of data in reducer task, which affects result of percentile_approx

CREATE TABLE bucket_n1 (key double, value string) CLUSTERED BY (key) SORTED BY (key DESC)  INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_n1;
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_n1;
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_n1;
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_n1;

create table t1_n132 (result double);
create table t2_n79 (result double);
create table t3_n31 (result double);
create table t4_n18 (result double);
create table t5_n5 (result double);
create table t6_n4 (result double);
create table t7_n5 (result array<double>);
create table t8_n3 (result array<double>);
create table t9_n2 (result array<double>);
create table t10_n1 (result array<double>);
create table t11_n3 (result array<double>);
create table t12_n1 (result array<double>);

set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.map.aggr=false;
-- disable map-side aggregation
FROM bucket_n1
insert overwrite table t1_n132 SELECT percentile_approx(cast(key AS double), 0.5)
insert overwrite table t2_n79 SELECT percentile_approx(cast(key AS double), 0.5, 100)
insert overwrite table t3_n31 SELECT percentile_approx(cast(key AS double), 0.5, 1000)

insert overwrite table t4_n18 SELECT percentile_approx(cast(key AS int), 0.5)
insert overwrite table t5_n5 SELECT percentile_approx(cast(key AS int), 0.5, 100)
insert overwrite table t6_n4 SELECT percentile_approx(cast(key AS int), 0.5, 1000)

insert overwrite table t7_n5 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98))
insert overwrite table t8_n3 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t9_n2 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 1000)

insert overwrite table t10_n1 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98))
insert overwrite table t11_n3 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t12_n1 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 1000);

select * from t1_n132;
select * from t2_n79;
select * from t3_n31;
select * from t4_n18;
select * from t5_n5;
select * from t6_n4;
select * from t7_n5;
select * from t8_n3;
select * from t9_n2;
select * from t10_n1;
select * from t11_n3;
select * from t12_n1;

set hive.map.aggr=true;
-- enable map-side aggregation
FROM bucket_n1
insert overwrite table t1_n132 SELECT percentile_approx(cast(key AS double), 0.5)
insert overwrite table t2_n79 SELECT percentile_approx(cast(key AS double), 0.5, 100)
insert overwrite table t3_n31 SELECT percentile_approx(cast(key AS double), 0.5, 1000)

insert overwrite table t4_n18 SELECT percentile_approx(cast(key AS int), 0.5)
insert overwrite table t5_n5 SELECT percentile_approx(cast(key AS int), 0.5, 100)
insert overwrite table t6_n4 SELECT percentile_approx(cast(key AS int), 0.5, 1000)

insert overwrite table t7_n5 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98))
insert overwrite table t8_n3 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t9_n2 SELECT percentile_approx(cast(key AS double), array(0.05,0.5,0.95,0.98), 1000)

insert overwrite table t10_n1 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98))
insert overwrite table t11_n3 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 100)
insert overwrite table t12_n1 SELECT percentile_approx(cast(key AS int), array(0.05,0.5,0.95,0.98), 1000);

select * from t1_n132;
select * from t2_n79;
select * from t3_n31;
select * from t4_n18;
select * from t5_n5;
select * from t6_n4;
select * from t7_n5;
select * from t8_n3;
select * from t9_n2;
select * from t10_n1;
select * from t11_n3;
select * from t12_n1;

-- NaN
explain
select percentile_approx(case when key < 100 then cast('NaN' as double) else key end, 0.5) from bucket_n1;
select percentile_approx(case when key < 100 then cast('NaN' as double) else key end, 0.5) between 340.5 and 343.0 from bucket_n1;

-- with CBO
explain
select percentile_approx(key, 0.5) from bucket_n1;
select percentile_approx(key, 0.5) between 255.0 and 257.0 from bucket_n1;
