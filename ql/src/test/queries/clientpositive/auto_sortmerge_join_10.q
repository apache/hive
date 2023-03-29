--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
;

set hive.exec.reducers.max = 1;
set hive.auto.convert.anti.join=true;

CREATE TABLE tbl1_n5(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2_n4(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1_n5
select * from src where key < 10;

insert overwrite table tbl2_n4
select * from src where key < 10;

set hive.auto.convert.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
-- disable hash joins
set hive.auto.convert.join.noconditionaltask.size=1;

-- One of the subqueries contains a union, so it should not be converted to a sort-merge join.
explain
select count(*) from 
  (
  select * from
  (select a.key as key, a.value as value from tbl1_n5 a where key < 6
     union all
   select a.key as key, a.value as value from tbl1_n5 a where key < 6
  ) usubq1 ) subq1
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (
  select * from
  (select a.key as key, a.value as value from tbl1_n5 a where key < 6
     union all
   select a.key as key, a.value as value from tbl1_n5 a where key < 6
  ) usubq1 ) subq1
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key;

set hive.auto.convert.sortmerge.join=true;

-- One of the subqueries contains a groupby, so it should not be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, count(*) as value from tbl1_n5 a where key < 6 group by a.key) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, count(*) as value from tbl1_n5 a where key < 6 group by a.key) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key;


set hive.auto.convert.sortmerge.join=true;
set hive.optimize.semijoin.conversion = false;

explain
select count(*) from
  (select a.key as key, count(*) as value from tbl1_n5 a where key < 6 group by a.key) subq1
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from
  (select a.key as key, count(*) as value from tbl1_n5 a where key < 6 group by a.key) subq1
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key;

explain
select subq2.key from
  (select a.key as key, count(*) as value from tbl1_n5 a where key < 6 group by a.key) subq1
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key order by subq2.key;

select subq2.key from
  (select a.key as key, count(*) as value from tbl1_n5 a where key < 6 group by a.key) subq1
    join
  (select a.key as key, a.value as value from tbl2_n4 a where key < 6) subq2
  on subq1.key = subq2.key order by subq2.key;

explain
select count(t1.key) from tbl1_n5 as t1 where not exists
    (select 1 from tbl2_n4 as t2 where t1.key = t2.key);

select count(t1.key) from tbl1_n5 as t1 where not exists
     (select 1 from tbl2_n4 as t2 where t1.key = t2.key);

set hive.auto.convert.anti.join=false;

explain
select count(t1.key) from tbl1_n5 as t1 where not exists
    (select 1 from tbl2_n4 as t2 where t1.key = t2.key);

select count(t1.key) from tbl1_n5 as t1 where not exists
     (select 1 from tbl2_n4 as t2 where t1.key = t2.key);
