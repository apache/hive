--! qt:dataset:src_thrift
--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

-- union2.q

-- union case: both subqueries are map-reduce jobs on same input, followed by reduce sink

explain 
  select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                        select s2.key as key, s2.value as value from src s2) unionsrc;

select count(1) FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                      select s2.key as key, s2.value as value from src s2) unionsrc;

-- union6.q

 

-- union case: 1 subquery is a map-reduce job, different inputs for sub-queries, followed by filesink

drop table if exists tmptable_n0;

create table tmptable_n0(key string, value string);

explain 
insert overwrite table tmptable_n0
  select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                        UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src1 s2) unionsrc;

insert overwrite table tmptable_n0
select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                                      UNION DISTINCT  
                                          select s2.key as key, s2.value as value from src1 s2) unionsrc;

select * from tmptable_n0 x sort by x.key, x.value;

drop table if exists tmptable_n0;


-- union8.q

-- union case: all subqueries are a map-only jobs, 3 way union, same input for all sub-queries, followed by filesink

explain 
  select unionsrc.key, unionsrc.value FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                                            select s2.key as key, s2.value as value from src s2 UNION DISTINCT  
                                            select s3.key as key, s3.value as value from src s3) unionsrc;

select unionsrc.key, unionsrc.value FROM (select s1.key as key, s1.value as value from src s1 UNION DISTINCT  
                                          select s2.key as key, s2.value as value from src s2 UNION DISTINCT  
                                          select s3.key as key, s3.value as value from src s3) unionsrc;

-- union11.q

 
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

-- union14.q

 
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

EXPLAIN
SELECT count(1) FROM (
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
  SELECT key, value FROM src) src;

-- union20.q

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

