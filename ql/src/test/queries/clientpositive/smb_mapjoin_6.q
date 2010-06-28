set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

drop table smb_bucket4_1;
CREATE TABLE smb_bucket4_1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS RCFILE;

drop table smb_bucket4_2;
CREATE TABLE smb_bucket4_2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS RCFILE;

create table smb_join_results(k1 int, v1 string, k2 int, v2 string);
create table normal_join_results(k1 int, v1 string, k2 int, v2 string);

insert overwrite table smb_bucket4_1
select * from src;

insert overwrite table smb_bucket4_2
select * from src;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

explain
insert overwrite table smb_join_results
select /*+mapjoin(a)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key;

insert overwrite table smb_join_results
select /*+mapjoin(a)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key;

select * from smb_join_results order by k1;

insert overwrite table normal_join_results select * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key;

select sum(hash(k1)) as k1, sum(hash(k2)) as k2, sum(hash(v1)) as v1, sum(hash(v2)) as v2 from normal_join_results;
select sum(hash(k1)) as k1, sum(hash(k2)) as k2, sum(hash(v1)) as v1, sum(hash(v2)) as v2 from smb_join_results;


explain
insert overwrite table smb_join_results
select /*+mapjoin(b)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key;
insert overwrite table smb_join_results
select /*+mapjoin(b)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key;


insert overwrite table smb_join_results
select /*+mapjoin(a)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key;

select * from smb_join_results order by k1;

insert overwrite table normal_join_results select * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key;

select sum(hash(k1)) as k1, sum(hash(k2)) as k2, sum(hash(v1)) as v1, sum(hash(v2)) as v2 from normal_join_results; 
select sum(hash(k1)) as k1, sum(hash(k2)) as k2, sum(hash(v1)) as v1, sum(hash(v2)) as v2 from smb_join_results;


explain
insert overwrite table smb_join_results
select /*+mapjoin(a)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key where a.key>1000;
insert overwrite table smb_join_results
select /*+mapjoin(a)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key where a.key>1000;


explain
insert overwrite table smb_join_results
select /*+mapjoin(b)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key where a.key>1000;
insert overwrite table smb_join_results
select /*+mapjoin(b)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key where a.key>1000;


explain
select /*+mapjoin(b,c)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key join smb_bucket4_2 c on b.key = c.key where a.key>1000;
select /*+mapjoin(b,c)*/ * from smb_bucket4_1 a join smb_bucket4_2 b on a.key = b.key join smb_bucket4_2 c on b.key = c.key where a.key>1000;

drop table smb_join_results;
drop table normal_join_results;
drop table smb_bucket4_1;
drop table smb_bucket4_2;
