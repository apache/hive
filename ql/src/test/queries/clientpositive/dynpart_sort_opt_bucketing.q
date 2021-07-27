set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=false;

drop table if exists t1_staging;
create table t1_staging(
a string,
b int,
c int,
d string)
partitioned by (e  string)
clustered by(a)
sorted by(a desc)
into 256 buckets stored as textfile;

load data local inpath '../../data/files/sortdp/000000_0' overwrite into table t1_staging partition (e='epart');



drop table t1_n147;

create table t1_n147(
a string,
b int,
c int,
d string)
partitioned by (e string)
clustered by(a)
sorted by(a desc) into 10 buckets stored as textfile;

insert overwrite table t1_n147 partition(e) select a,b,c,d,'epart' from t1_staging;

select 'bucket_0';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000000_0;
select 'bucket_2';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000002_0;
select 'bucket_4';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000004_0;
select 'bucket_6';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000006_0;
select 'bucket_8';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000008_0;

-- disable sorted dynamic partition optimization to make sure the results are correct
drop table t1_n147;

create table t1_n147(
a string,
b int,
c int,
d string)
partitioned by (e string)
clustered by(a)
sorted by(a desc) into 10 buckets stored as textfile;

insert overwrite table t1_n147 partition(e) select a,b,c,d,'epart' from t1_staging;

select 'bucket_0';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000000_0;
select 'bucket_2';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000002_0;
select 'bucket_4';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000004_0;
select 'bucket_6';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000006_0;
select 'bucket_8';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1_n147/e=epart/000008_0;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.autogather=false;
set hive.optimize.sort.dynamic.partition.threshold=1;

CREATE TABLE dynpart_sort_opt_bucketing_test (ca_address_sk int, ca_address_id string, ca_street_number string, ca_street_name string,
    ca_street_type string, ca_suite_number string, ca_city string, ca_county string, ca_state string,
    ca_zip string, ca_country string, ca_gmt_offset decimal(5,2))
    PARTITIONED BY (ca_location_type string)
    CLUSTERED BY (ca_state) INTO 50 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');

explain INSERT INTO TABLE dynpart_sort_opt_bucketing_test PARTITION (ca_location_type) VALUES (5555, 'AAAAAAAADLFBAAAA', '126',
                'Highland Park', 'Court', 'Suite E', 'San Jose', 'King George County', 'VA', '28003', 'United States',
                '-5', 'single family');

INSERT INTO TABLE dynpart_sort_opt_bucketing_test PARTITION (ca_location_type) VALUES (5555, 'AAAAAAAADLFBAAAA', '126',
                'Highland Park', 'Court', 'Suite E', 'San Jose', 'King George County', 'VA', '28003', 'United States',
                '-5', 'single family');
select * from dynpart_sort_opt_bucketing_test;

-- with auto stats
set hive.stats.autogather=true;
explain INSERT INTO TABLE dynpart_sort_opt_bucketing_test PARTITION (ca_location_type) VALUES (5555, 'AAAAAAAADLFBAAAA', '126',
                   'Highland Park', 'Court', 'Suite E', 'San Jose', 'King George County', 'VA', '28003', 'United States',
                   '-5', 'single family');
INSERT INTO TABLE dynpart_sort_opt_bucketing_test PARTITION (ca_location_type) VALUES (5555, 'AAAAAAAADLFBAAAA', '126',
                   'Highland Park', 'Court', 'Suite E', 'San Jose', 'King George County', 'VA', '28003', 'United States',
                   '-5', 'single family');
select * from dynpart_sort_opt_bucketing_test;

DROP TABLE dynpart_sort_opt_bucketing_test;

-- test case to test that CAST on bucketing column doesn't prevent sort dynamic partition

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists t1_staging;
create table t1_staging(
                           a string,
                           b int,
                           c int,
                           d string)
    partitioned by (e  decimal(18,0))
    clustered by(a)
        into 256 buckets STORED AS TEXTFILE;
load data local inpath '../../data/files/sortdp/000000_0' overwrite into table t1_staging partition (e=100);

drop table t1_n147;
create table t1_n147(
                        a string,
                        b decimal(6,0),
                        c int,
                        d string)
    partitioned by (e decimal(3,0))
    clustered by(a,b)
        into 10 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

set hive.stats.autogather=false;
set hive.optimize.bucketingsorting = true;
explain insert overwrite table t1_n147 partition(e) select a,b,c,d,e  from t1_staging;
insert overwrite table t1_n147 partition(e) select a,b,c,d,e  from t1_staging;

with q1 as (select count(*) as cnt from t1_staging),
    q2 as (select count(*) as cnt from t1_n147)
select q1.cnt = q2.cnt from q1 join q2;

drop table t1_staging;
drop table t1_n147;




