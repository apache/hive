--! qt:dataset:
-- SORT_QUERY_RESULTS

set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.optimize.bucketingsorting=true;

create table bucket1 (id int, val string) clustered by (id) sorted by (id ASC) INTO 4 BUCKETS;
insert into bucket1 values (1, 'abc'), (3, 'abc');
select * from bucket1;

create table bucket2 like bucket1;
insert overwrite table bucket2 select * from bucket1;
select * from bucket2;
