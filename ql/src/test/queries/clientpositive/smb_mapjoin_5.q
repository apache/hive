set hive.strict.checks.bucketing=false;





create table smb_bucket_1_n2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS STORED AS RCFILE; 
create table smb_bucket_2_n2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS STORED AS RCFILE; 
create table smb_bucket_3_n2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS STORED AS RCFILE;

load data local inpath '../../data/files/smb_rc1/000000_0' overwrite into table smb_bucket_1_n2;
load data local inpath '../../data/files/smb_rc2/000000_0' overwrite into table smb_bucket_2_n2;
load data local inpath '../../data/files/smb_rc3/000000_0' overwrite into table smb_bucket_3_n2;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

-- SORT_QUERY_RESULTS

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key left outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key left outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key right outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key right outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key full outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a left outer join smb_bucket_2_n2 b on a.key = b.key full outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key left outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key left outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key right outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key right outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key full outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a right outer join smb_bucket_2_n2 b on a.key = b.key full outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key left outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key left outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key right outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key right outer join smb_bucket_3_n2 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key full outer join smb_bucket_3_n2 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1_n2 a full outer join smb_bucket_2_n2 b on a.key = b.key full outer join smb_bucket_3_n2 c on b.key=c.key;

 



