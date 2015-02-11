-- union10.q

 

-- union case: all subqueries are a map-reduce jobs, 3 way union, same input for all sub-queries, followed by filesink

create table tmptable(key string, value int);

explain 
insert overwrite table tmptable
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                            select 'tst2' as key, count(1) as value from src s2
                                        UNION DISTINCT
                                            select 'tst3' as key, count(1) as value from src s3) unionsrc;


insert overwrite table tmptable
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                            select 'tst2' as key, count(1) as value from src s2
                                        UNION DISTINCT
                                            select 'tst3' as key, count(1) as value from src s3) unionsrc;


select * from tmptable x sort by x.key;

-- union11.q

 
-- SORT_BEFORE_DIFF
-- union case: all subqueries are a map-reduce jobs, 3 way union, same input for all sub-queries, followed by reducesink

explain 
  select unionsrc.key, count(1) FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                            select 'tst2' as key, count(1) as value from src s2
                                        UNION DISTINCT
                                            select 'tst3' as key, count(1) as value from src s3) unionsrc group by unionsrc.key;


  select unionsrc.key, count(1) FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                            select 'tst2' as key, count(1) as value from src s2
                                        UNION DISTINCT
                                            select 'tst3' as key, count(1) as value from src s3) unionsrc group by unionsrc.key;



-- union12.q

 

-- union case: all subqueries are a map-reduce jobs, 3 way union, different inputs for all sub-queries, followed by filesink

create table tmptable12(key string, value int);

explain 
insert overwrite table tmptable12
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                            select 'tst2' as key, count(1) as value from src1 s2
                                        UNION DISTINCT
                                            select 'tst3' as key, count(1) as value from srcbucket s3) unionsrc;


insert overwrite table tmptable12
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                            select 'tst2' as key, count(1) as value from src1 s2
                                        UNION DISTINCT
                                            select 'tst3' as key, count(1) as value from srcbucket s3) unionsrc;

select * from tmptable12 x sort by x.key;
-- union13.q

-- SORT_BEFORE_DIFF
-- union case: both subqueries are a map-only jobs, same input, followed by filesink

explain 
  select unionsrc.key, unionsrc.value FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src s2) unionsrc;

select unionsrc.key, unionsrc.value FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                                          select s2.key as key, s2.value as value from src s2) unionsrc;
-- union14.q

 
-- SORT_BEFORE_DIFF
-- union case: 1 subquery is a map-reduce job, different inputs for sub-queries, followed by reducesink

explain 
  select unionsrc.key, count(1) FROM (select s2.key as key, s2.value as value from src1 s2
                                        UNION DISTINCT  
                                      select 'tst1' as key, cast(count(1) as string) as value from src s1) 
  unionsrc group by unionsrc.key;



  select unionsrc.key, count(1) FROM (select s2.key as key, s2.value as value from src1 s2
                                        UNION DISTINCT  
                                      select 'tst1' as key, cast(count(1) as string) as value from src s1) 
  unionsrc group by unionsrc.key;
-- union15.q

 
-- SORT_BEFORE_DIFF
-- union case: 1 subquery is a map-reduce job, different inputs for sub-queries, followed by reducesink

explain 
  select unionsrc.key, count(1) FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                        UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src1 s2
                                        UNION DISTINCT  
                                            select s3.key as key, s3.value as value from src1 s3) unionsrc group by unionsrc.key;

  select unionsrc.key, count(1) FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                        UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src1 s2
                                        UNION DISTINCT  
                                            select s3.key as key, s3.value as value from src1 s3) unionsrc group by unionsrc.key;


-- union16.q

-- SORT_BEFORE_DIFF
EXPLAIN
SELECT count(1) FROM (
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src) src;


SELECT count(1) FROM (
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT

  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src UNION DISTINCT
  SELECT key, value FROM src) src;
-- union17.q

CREATE TABLE DEST1(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- SORT_BEFORE_DIFF
-- union case:map-reduce sub-queries followed by multi-table insert

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key, unionsrc.value;

SELECT DEST1.* FROM DEST1;
SELECT DEST2.* FROM DEST2;
-- union18.q

CREATE TABLE DEST118(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST218(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- union case:map-reduce sub-queries followed by multi-table insert 

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST118 SELECT unionsrc.key, unionsrc.value
INSERT OVERWRITE TABLE DEST218 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST118 SELECT unionsrc.key, unionsrc.value
INSERT OVERWRITE TABLE DEST218 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

SELECT DEST118.* FROM DEST118 SORT BY DEST118.key, DEST118.value;
SELECT DEST218.* FROM DEST218 SORT BY DEST218.key, DEST218.val1, DEST218.val2;
-- union19.q




CREATE TABLE DEST119(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST219(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- union case:map-reduce sub-queries followed by multi-table insert

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST119 SELECT unionsrc.key, count(unionsrc.value) group by unionsrc.key
INSERT OVERWRITE TABLE DEST219 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST119 SELECT unionsrc.key, count(unionsrc.value) group by unionsrc.key
INSERT OVERWRITE TABLE DEST219 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

SELECT DEST119.* FROM DEST119 SORT BY DEST119.key, DEST119.value;
SELECT DEST219.* FROM DEST219 SORT BY DEST219.key, DEST219.val1, DEST219.val2;



-- union2.q

-- SORT_BEFORE_DIFF
-- union case: both subqueries are map-reduce jobs on same input, followed by reduce sink

explain 
  select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                        select s2.key as key, s2.value as value from src s2) unionsrc;

select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                      select s2.key as key, s2.value as value from src s2) unionsrc;
-- union20.q

-- SORT_BEFORE_DIFF
-- union :map-reduce sub-queries followed by join

explain 
SELECT unionsrc1.key, unionsrc1.value, unionsrc2.key, unionsrc2.value
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2 where s2.key < 10) unionsrc1 
JOIN 
     (select 'tst1' as key, cast(count(1) as string) as value from src s3
                         UNION DISTINCT  
      select s4.key as key, s4.value as value from src s4 where s4.key < 10) unionsrc2
ON (unionsrc1.key = unionsrc2.key);

SELECT unionsrc1.key, unionsrc1.value, unionsrc2.key, unionsrc2.value
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2 where s2.key < 10) unionsrc1 
JOIN 
     (select 'tst1' as key, cast(count(1) as string) as value from src s3
                         UNION DISTINCT  
      select s4.key as key, s4.value as value from src s4 where s4.key < 10) unionsrc2
ON (unionsrc1.key = unionsrc2.key);
-- union21.q

-- SORT_BEFORE_DIFF
-- union of constants, udf outputs, and columns from text table and thrift table

explain
SELECT key, count(1)
FROM (
  SELECT '1' as key from src
  UNION DISTINCT
  SELECT reverse(key) as key from src
  UNION DISTINCT
  SELECT key as key from src
  UNION DISTINCT
  SELECT astring as key from src_thrift
  UNION DISTINCT
  SELECT lstring[0] as key from src_thrift
) union_output
GROUP BY key;

SELECT key, count(1)
FROM (
  SELECT '1' as key from src
  UNION DISTINCT
  SELECT reverse(key) as key from src
  UNION DISTINCT
  SELECT key as key from src
  UNION DISTINCT
  SELECT astring as key from src_thrift
  UNION DISTINCT
  SELECT lstring[0] as key from src_thrift
) union_output
GROUP BY key;
-- union22.q

-- SORT_QUERY_RESULTS

create table dst_union22(k1 string, k2 string, k3 string, k4 string) partitioned by (ds string);
create table dst_union22_delta(k0 string, k1 string, k2 string, k3 string, k4 string, k5 string) partitioned by (ds string);

insert overwrite table dst_union22 partition (ds='1')
select key, value, key , value from src;

insert overwrite table dst_union22_delta partition (ds='1')
select key, key, value, key, value, value from src;

set hive.merge.mapfiles=false;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- Since the inputs are small, it should be automatically converted to mapjoin

explain extended
insert overwrite table dst_union22 partition (ds='2')
select * from
(
select k1 as k1, k2 as k2, k3 as k3, k4 as k4 from dst_union22_delta where ds = '1' and k0 <= 50
UNION DISTINCT
select a.k1 as k1, a.k2 as k2, b.k3 as k3, b.k4 as k4
from dst_union22 a left outer join (select * from dst_union22_delta where ds = '1' and k0 > 50) b on
a.k1 = b.k1 and a.ds='1'
where a.k1 > 20
)
subq;

insert overwrite table dst_union22 partition (ds='2')
select * from
(
select k1 as k1, k2 as k2, k3 as k3, k4 as k4 from dst_union22_delta where ds = '1' and k0 <= 50
UNION DISTINCT
select a.k1 as k1, a.k2 as k2, b.k3 as k3, b.k4 as k4
from dst_union22 a left outer join (select * from dst_union22_delta where ds = '1' and k0 > 50) b on
a.k1 = b.k1 and a.ds='1'
where a.k1 > 20
)
subq;

select * from dst_union22 where ds = '2';
-- union23.q

explain
select s.key2, s.value2
from (
  select transform(key, value) using 'cat' as (key2, value2)
  from src
  UNION DISTINCT 
  select key as key2, value as value2 from src) s
order by s.key2, s.value2;

select s.key2, s.value2
from (
  select transform(key, value) using 'cat' as (key2, value2)
  from src
  UNION DISTINCT 
  select key as key2, value as value2 from src) s
order by s.key2, s.value2;

-- union24.q

-- SORT_QUERY_RESULTS

create table src2 as select key, count(1) as count from src group by key;
create table src3 as select * from src2;
create table src4 as select * from src2;
create table src5 as select * from src2;


set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;


explain extended
select s.key, s.count from (
  select key, count from src2  where key < 10
  UNION DISTINCT
  select key, count from src3  where key < 10
  UNION DISTINCT
  select key, count from src4  where key < 10
  UNION DISTINCT
  select key, count(1) as count from src5 where key < 10 group by key
)s
;

select s.key, s.count from (
  select key, count from src2  where key < 10
  UNION DISTINCT
  select key, count from src3  where key < 10
  UNION DISTINCT
  select key, count from src4  where key < 10
  UNION DISTINCT
  select key, count(1) as count from src5 where key < 10 group by key
)s
;

explain extended
select s.key, s.count from (
  select key, count from src2  where key < 10
  UNION DISTINCT
  select key, count from src3  where key < 10
  UNION DISTINCT
  select a.key as key, b.count as count from src4 a join src5 b on a.key=b.key where a.key < 10
)s
;

select s.key, s.count from (
  select key, count from src2  where key < 10
  UNION DISTINCT
  select key, count from src3  where key < 10
  UNION DISTINCT
  select a.key as key, b.count as count from src4 a join src5 b on a.key=b.key where a.key < 10
)s
;

explain extended
select s.key, s.count from (
  select key, count from src2  where key < 10
  UNION DISTINCT
  select key, count from src3  where key < 10
  UNION DISTINCT
  select a.key as key, count(1) as count from src4 a join src5 b on a.key=b.key where a.key < 10 group by a.key
)s
;

select s.key, s.count from (
  select key, count from src2  where key < 10
  UNION DISTINCT
  select key, count from src3  where key < 10
  UNION DISTINCT
  select a.key as key, count(1) as count from src4 a join src5 b on a.key=b.key where a.key < 10 group by a.key
)s
;
-- union25.q

create table tmp_srcpart like srcpart;

insert overwrite table tmp_srcpart partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

explain
create table tmp_unionall as
SELECT count(1) as counts, key, value
FROM
(
  SELECT key, value FROM srcpart a WHERE a.ds='2008-04-08' and a.hr='11'

    UNION DISTINCT

  SELECT key, key as value FROM (
    SELECT distinct key FROM (
      SELECT key, value FROM tmp_srcpart a WHERE a.ds='2008-04-08' and a.hr='11'
        UNION DISTINCT
      SELECT key, value FROM tmp_srcpart b WHERE b.ds='2008-04-08' and b.hr='11'
    )t
  ) master_table
) a GROUP BY key, value
;
-- union26.q

-- SORT_QUERY_RESULTS

EXPLAIN
SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION DISTINCT

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value
;

SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION DISTINCT

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value
;


SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION DISTINCT

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value
;

SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION DISTINCT

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value
;
-- union27.q

-- SORT_BEFORE_DIFF
create table jackson_sev_same as select * from src;
create table dim_pho as select * from src;
create table jackson_sev_add as select * from src;
explain select b.* from jackson_sev_same a join (select * from dim_pho UNION DISTINCT select * from jackson_sev_add)b on a.key=b.key and b.key=97;
select b.* from jackson_sev_same a join (select * from dim_pho UNION DISTINCT select * from jackson_sev_add)b on a.key=b.key and b.key=97;
-- union28.q

create table union_subq_union(key int, value string);

explain
insert overwrite table union_subq_union 
select * from (
  select key, value from src 
  UNION DISTINCT 
  select key, value from 
  (
    select key, value, count(1) from src group by key, value
    UNION DISTINCT
    select key, value, count(1) from src group by key, value
  ) subq
) a
;

insert overwrite table union_subq_union 
select * from (
  select key, value from src 
  UNION DISTINCT 
  select key, value from 
  (
    select key, value, count(1) from src group by key, value
    UNION DISTINCT
    select key, value, count(1) from src group by key, value
  ) subq
) a
;

select * from union_subq_union order by key, value limit 20;
-- union29.q

create table union_subq_union29(key int, value string);

explain
insert overwrite table union_subq_union29 
select * from (
  select key, value from src 
  UNION DISTINCT 
  select key, value from 
  (
    select key, value from src 
    UNION DISTINCT
    select key, value from src
  ) subq
) a
;

insert overwrite table union_subq_union29 
select * from (
  select key, value from src 
  UNION DISTINCT 
  select key, value from 
  (
    select key, value from src 
    UNION DISTINCT
    select key, value from src
  ) subq
) a
;

select * from union_subq_union29 order by key, value limit 20;
-- union3.q

-- SORT_BEFORE_DIFF

explain
SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION DISTINCT
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION DISTINCT
  SELECT 3 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  UNION DISTINCT
  SELECT 4 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  CLUSTER BY id
) a;



CREATE TABLE union_out (id int);

insert overwrite table union_out 
SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION DISTINCT
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION DISTINCT
  SELECT 3 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  UNION DISTINCT
  SELECT 4 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  CLUSTER BY id
) a;

select * from union_out;
-- union30.q

create table union_subq_union30(key int, value string);

explain
insert overwrite table union_subq_union30 
select * from (

select * from (
  select key, value from src 
  UNION DISTINCT 
  select key, value from 
  (
    select key, value, count(1) from src group by key, value
    UNION DISTINCT
    select key, value, count(1) from src group by key, value
  ) subq
) a

UNION DISTINCT

select key, value from src
) aa
;

insert overwrite table union_subq_union30 
select * from (

select * from (
  select key, value from src 
  UNION DISTINCT 
  select key, value from 
  (
    select key, value, count(1) from src group by key, value
    UNION DISTINCT
    select key, value, count(1) from src group by key, value
  ) subq
) a

UNION DISTINCT

select key, value from src
) aa
;

select * from union_subq_union30 order by key, value limit 20;
-- union31.q

-- SORT_QUERY_RESULTS

drop table t1;
drop table t2;


create table t1 as select * from src where key < 10;
create table t2 as select * from src where key < 10;

create table t3(key string, cnt int);
create table t4(value string, cnt int);

explain
from
(select * from t1
 UNION DISTINCT
 select * from t2
) x
insert overwrite table t3
  select key, count(1) group by key
insert overwrite table t4
  select value, count(1) group by value;

from
(select * from t1
 UNION DISTINCT
 select * from t2
) x
insert overwrite table t3
  select key, count(1) group by key
insert overwrite table t4
  select value, count(1) group by value;

select * from t3;
select * from t4;

create table t5(c1 string, cnt int);
create table t6(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1 group by key
   UNION DISTINCT
 select key as c1, count(1) as cnt from t2 group by key
) x
insert overwrite table t5
  select c1, sum(cnt) group by c1
insert overwrite table t6
  select c1, sum(cnt) group by c1;

from
(
 select key as c1, count(1) as cnt from t1 group by key
   UNION DISTINCT
 select key as c1, count(1) as cnt from t2 group by key
) x
insert overwrite table t5
  select c1, sum(cnt) group by c1
insert overwrite table t6
  select c1, sum(cnt) group by c1;

select * from t5;
select * from t6;

drop table t1;
drop table t2;

create table t1 as select * from src where key < 10;
create table t2 as select key, count(1) as cnt from src where key < 10 group by key;

create table t7(c1 string, cnt int);
create table t8(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1 group by key
   UNION DISTINCT
 select key as c1, cnt from t2
) x
insert overwrite table t7
  select c1, count(1) group by c1
insert overwrite table t8
  select c1, count(1) group by c1;

from
(
 select key as c1, count(1) as cnt from t1 group by key
   UNION DISTINCT
 select key as c1, cnt from t2
) x
insert overwrite table t7
  select c1, count(1) group by c1
insert overwrite table t8
  select c1, count(1) group by c1;

select * from t7;
select * from t8;
-- union32.q

-- SORT_QUERY_RESULTS

-- This tests various union queries which have columns on one side of the query
-- being of double type and those on the other side another

drop table if exists t1;

drop table if exists t2;

CREATE TABLE t1 AS SELECT * FROM src WHERE key < 10;
CREATE TABLE t2 AS SELECT * FROM src WHERE key < 10;

-- Test simple union with double
EXPLAIN
SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t1
UNION DISTINCT
SELECT CAST(key AS BIGINT) AS key FROM t2) a
ORDER BY key;

SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t1
UNION DISTINCT
SELECT CAST(key AS BIGINT) AS key FROM t2) a
;

-- Test union with join on the left
EXPLAIN
SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key FROM t1 a JOIN t2 b ON a.key = b.key
UNION DISTINCT
SELECT CAST(key AS DOUBLE) AS key FROM t2) a
;

SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key FROM t1 a JOIN t2 b ON a.key = b.key
UNION DISTINCT
SELECT CAST(key AS DOUBLE) AS key FROM t2) a
;

-- Test union with join on the right
EXPLAIN
SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t2
UNION DISTINCT
SELECT CAST(a.key AS BIGINT) AS key FROM t1 a JOIN t2 b ON a.key = b.key) a
;

SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t2
UNION DISTINCT
SELECT CAST(a.key AS BIGINT) AS key FROM t1 a JOIN t2 b ON a.key = b.key) a
;

-- Test union with join on the left selecting multiple columns
EXPLAIN
SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS DOUBLE) AS value FROM t1 a JOIN t2 b ON a.key = b.key
UNION DISTINCT
SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2) a
;

SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS DOUBLE) AS value FROM t1 a JOIN t2 b ON a.key = b.key
UNION DISTINCT
SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2) a
;

-- Test union with join on the right selecting multiple columns
EXPLAIN
SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2
UNION DISTINCT
SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS DOUBLE) AS value FROM t1 a JOIN t2 b ON a.key = b.key) a
;

SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2
UNION DISTINCT
SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS DOUBLE) AS value FROM t1 a JOIN t2 b ON a.key = b.key) a
;
-- union33.q

-- SORT_BEFORE_DIFF
-- This tests that a UNION DISTINCT with a map only subquery on one side and a 
-- subquery involving two map reduce jobs on the other runs correctly.

drop table if exists test_src;

CREATE TABLE test_src (key STRING, value STRING);

EXPLAIN INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, value FROM src 
	WHERE key = 0
UNION DISTINCT
 	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
)a;
 
INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, value FROM src 
	WHERE key = 0
UNION DISTINCT
 	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
)a;
 
SELECT COUNT(*) FROM test_src;
 
EXPLAIN INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
UNION DISTINCT
 	SELECT key, value FROM src 
	WHERE key = 0
)a;
 
INSERT OVERWRITE TABLE test_src 
SELECT key, value FROM (
	SELECT key, COUNT(*) AS value FROM src
 	GROUP BY key
UNION DISTINCT
 	SELECT key, value FROM src 
	WHERE key = 0
)a;
 
SELECT COUNT(*) FROM test_src;
 -- union34.q

create table src10_1 (key string, value string);
create table src10_2 (key string, value string);
create table src10_3 (key string, value string);
create table src10_4 (key string, value string);

from (select * from src tablesample (10 rows)) a
insert overwrite table src10_1 select *
insert overwrite table src10_2 select *
insert overwrite table src10_3 select *
insert overwrite table src10_4 select *;

set hive.auto.convert.join=true;
-- When we convert the Join of sub1 and sub0 into a MapJoin,
-- we can use a single MR job to evaluate this entire query.
explain
SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION DISTINCT
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION DISTINCT SELECT * FROM src10_4 ) alias0
) alias1 order by key;

SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION DISTINCT
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION DISTINCT SELECT * FROM src10_4 ) alias0
) alias1 order by key;

set hive.auto.convert.join=false;
-- When we do not convert the Join of sub1 and sub0 into a MapJoin,
-- we need to use two MR jobs to evaluate this query.
-- The first job is for the Join of sub1 and sub2. The second job
-- is for the UNION DISTINCT and ORDER BY.
explain
SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION DISTINCT
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION DISTINCT SELECT * FROM src10_4 ) alias0
) alias1 order by key;

SELECT * FROM (
  SELECT sub1.key,sub1.value FROM (SELECT * FROM src10_1) sub1 JOIN (SELECT * FROM src10_2) sub0 ON (sub0.key = sub1.key)
  UNION DISTINCT
  SELECT key,value FROM (SELECT * FROM (SELECT * FROM src10_3) sub2 UNION DISTINCT SELECT * FROM src10_4 ) alias0
) alias1 order by key;
-- union4.q

 

-- union case: both subqueries are map-reduce jobs on same input, followed by filesink

drop table if exists tmptable;

create table tmptable(key string, value int);

explain 
insert overwrite table tmptable
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                            select 'tst2' as key, count(1) as value from src s2) unionsrc;

insert overwrite table tmptable
select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src s1
                                        UNION DISTINCT  
                                          select 'tst2' as key, count(1) as value from src s2) unionsrc;

select * from tmptable x sort by x.key;


-- union5.q

 
-- SORT_BEFORE_DIFF
-- union case: both subqueries are map-reduce jobs on same input, followed by reduce sink

explain 
  select unionsrc.key, count(1) FROM (select 'tst1' as key, count(1) as value from src s1
                                    UNION DISTINCT  
                                      select 'tst2' as key, count(1) as value from src s2) unionsrc group by unionsrc.key;

select unionsrc.key, count(1) FROM (select 'tst1' as key, count(1) as value from src s1
                                  UNION DISTINCT  
                                    select 'tst2' as key, count(1) as value from src s2) unionsrc group by unionsrc.key;
-- union6.q

 

-- union case: 1 subquery is a map-reduce job, different inputs for sub-queries, followed by filesink

drop table if exists tmptable;

create table tmptable(key string, value string);

explain 
insert overwrite table tmptable
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                        UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src1 s2) unionsrc;

insert overwrite table tmptable
select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                      UNION DISTINCT  
                                          select s2.key as key, s2.value as value from src1 s2) unionsrc;

select * from tmptable x sort by x.key, x.value;


-- union7.q

 

-- SORT_BEFORE_DIFF
-- union case: 1 subquery is a map-reduce job, different inputs for sub-queries, followed by reducesink

explain 
  select unionsrc.key, count(1) FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                        UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src1 s2) unionsrc group by unionsrc.key;

select unionsrc.key, count(1) FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                      UNION DISTINCT  
                                    select s2.key as key, s2.value as value from src1 s2) unionsrc group by unionsrc.key;


-- union8.q

-- SORT_BEFORE_DIFF
-- union case: all subqueries are a map-only jobs, 3 way union, same input for all sub-queries, followed by filesink

explain 
  select unionsrc.key, unionsrc.value FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src s2 UNION DISTINCT  
                                            select s3.key as key, s3.value as value from src s3) unionsrc;

select unionsrc.key, unionsrc.value FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                                          select s2.key as key, s2.value as value from src s2 UNION DISTINCT  
                                          select s3.key as key, s3.value as value from src s3) unionsrc;
-- union9.q

-- SORT_BEFORE_DIFF
-- union case: all subqueries are a map-only jobs, 3 way union, same input for all sub-queries, followed by reducesink

explain 
  select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                        select s2.key as key, s2.value as value from src s2 UNION DISTINCT
                        select s3.key as key, s3.value as value from src s3) unionsrc;

  select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                        select s2.key as key, s2.value as value from src s2 UNION DISTINCT
                        select s3.key as key, s3.value as value from src s3) unionsrc;
