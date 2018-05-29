set hive.strict.checks.bucketing=false;

set hive.auto.convert.join=false;

set hive.exec.dynamic.partition.mode=nonstrict;



set hive.auto.convert.sortmerge.join=false;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

-- SORT_QUERY_RESULTS

CREATE TABLE stage_bucket_big_n17
(
key BIGINT,
value STRING
)
PARTITIONED BY (file_tag STRING);

CREATE TABLE bucket_big_n17
(
key BIGINT,
value STRING
)
PARTITIONED BY (day STRING, pri bigint)
clustered by (key) sorted by (key) into 12 buckets
stored as RCFile;

CREATE TABLE stage_bucket_small_n17
(
key BIGINT,
value string
)
PARTITIONED BY (file_tag STRING);

CREATE TABLE bucket_small_n17
(
key BIGINT,
value string
)
PARTITIONED BY (pri bigint)
clustered by (key) sorted by (key) into 12 buckets
stored as RCFile;

load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' overwrite into table stage_bucket_small_n17 partition (file_tag='1');
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' overwrite into table stage_bucket_small_n17 partition (file_tag='2');

insert overwrite table bucket_small_n17 partition(pri) 
select 
key, 
value, 
file_tag as pri 
from 
stage_bucket_small_n17 
where file_tag between 1 and 2;

load data local inpath '../../data/files/auto_sortmerge_join/small/000000_0' overwrite into table stage_bucket_big_n17 partition (file_tag='1');

insert overwrite table bucket_big_n17 partition(day,pri)
 select key, value, 'day1' as day, 1 as pri
   from stage_bucket_big_n17
   where file_tag='1';

explain select a.key , a.value , b.value , 'day1' as day, 1 as pri
        from
        ( select key, value
          from bucket_big_n17 where day='day1' ) a
          left outer join
          ( select key, value
            from bucket_small_n17
            where pri between 1 and 2 ) b
            on
          (a.key = b.key)
;

select a.key , a.value , b.value , 'day1' as day, 1 as pri
from
( select key, value
  from bucket_big_n17 where day='day1' ) a
  left outer join
  ( select key, value
  from bucket_small_n17
  where pri between 1 and 2 ) b
on
(a.key = b.key) 
;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=1;
set hive.auto.convert.sortmerge.join=true;

explain select a.key , a.value , b.value , 'day1' as day, 1 as pri
from
( select key, value
  from bucket_big_n17 where day='day1' ) a
left outer join
( select key, value
  from bucket_small_n17
  where pri between 1 and 2 ) b
on
(a.key = b.key)
;

select a.key , a.value , b.value , 'day1' as day, 1 as pri
from
( select key, value
  from bucket_big_n17 where day='day1' ) a
left outer join
( select key, value
  from bucket_small_n17
  where pri between 1 and 2 ) b
on
(a.key = b.key)
;

drop table bucket_big_n17;
drop table bucket_small_n17;

-- Test to make sure SMB is not kicked in when small table has more buckets than big table

CREATE TABLE bucket_big_n17
(
key BIGINT,
value STRING
)
PARTITIONED BY (day STRING, pri bigint)
clustered by (key) sorted by (key) into 12 buckets
stored as RCFile;

CREATE TABLE bucket_small_n17
(
key BIGINT,
value string
)
PARTITIONED BY (pri bigint)
clustered by (key) sorted by (key) into 24 buckets
stored as RCFile;

insert overwrite table bucket_small_n17 partition(pri)
select
key,
value,
file_tag as pri
from
stage_bucket_small_n17
where file_tag between 1 and 2;

insert overwrite table bucket_big_n17 partition(day,pri)
select key, value, 'day1' as day, 1 as pri
from stage_bucket_big_n17
where file_tag='1';


explain select a.key , a.value , b.value , 'day1' as day, 1 as pri
        from
        ( select key, value
          from bucket_big_n17 where day='day1' ) a
        left outer join
        ( select key, value
          from bucket_small_n17
          where pri between 1 and 2 ) b
        on
        (a.key = b.key)
;

select a.key , a.value , b.value , 'day1' as day, 1 as pri
from
( select key, value
  from bucket_big_n17 where day='day1' ) a
left outer join
( select key, value
  from bucket_small_n17
  where pri between 1 and 2 ) b
on
(a.key = b.key)
;
