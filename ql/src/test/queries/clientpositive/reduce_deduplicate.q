set hive.enforce.bucketing = true;
set hive.exec.reducers.max = 1;
set hive.exec.script.trust = true;

drop table bucket5_1;

CREATE TABLE bucket5_1(key string, value string) CLUSTERED BY (key) INTO 2 BUCKETS;
explain extended
insert overwrite table bucket5_1
select * from src cluster by key;

insert overwrite table bucket5_1
select * from src cluster by key;

select sum(hash(key)),sum(hash(value)) from bucket5_1;
select sum(hash(key)),sum(hash(value)) from src;

drop table complex_tbl_1;
create table complex_tbl_1(aid string, bid string, t int, ctime string, etime bigint, l string, et string) partitioned by (ds string);

drop table complex_tbl_2;
create table complex_tbl_2(aet string, aes string) partitioned by (ds string);

explain extended
insert overwrite table complex_tbl_1 partition (ds='2010-03-29')
select s2.* from
(
 select TRANSFORM (aid,bid,t,ctime,etime,l,et)
 USING 'cat'
 AS (aid string, bid string, t int, ctime string, etime bigint, l string, et string)
 from
  (
   select transform(aet,aes)
   using 'cat'
   as (aid string, bid string, t int, ctime string, etime bigint, l string, et string)
   from complex_tbl_2 where ds ='2010-03-29' cluster by bid
)s
)s2;

drop table complex_tbl_2;
drop table complex_tbl_1;
drop table bucket5_1;

