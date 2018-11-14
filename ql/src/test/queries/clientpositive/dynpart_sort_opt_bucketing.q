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

set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;



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

set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;



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
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.autogather=false;

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
DROP TABLE dynpart_sort_opt_bucketing_test;


