--! qt:dataset:alltypesorc
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled = true;

explain vectorization detail
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else "c"
  end,
  case csmallint
    when 418 then "a"
    when 12205 then "b"
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else "c"
  end,
  case csmallint
    when 418 then "a"
    when 12205 then "b"
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
explain vectorization detail
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else null
  end,
  case csmallint
    when 418 then "a"
    when 12205 then null
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
explain vectorization detail
select
  sum(case when cint % 2 = 0 then 1 else 0 end) as ceven,
  sum(case when cint % 2 = 1 then 1 else 0 end) as codd
from alltypesorc;
select
  sum(case when cint % 2 = 0 then 1 else 0 end) as ceven,
  sum(case when cint % 2 = 1 then 1 else 0 end) as codd
from alltypesorc;
explain vectorization detail
select
  sum(case when cint % 2 = 0 then cint else 0 end) as ceven,
  sum(case when cint % 2 = 1 then cint else 0 end) as codd
from alltypesorc;
select
  sum(case when cint % 2 = 0 then cint else 0 end) as ceven,
  sum(case when cint % 2 = 1 then cint else 0 end) as codd
from alltypesorc;

-- add test for VectorUDFAdaptor call IfExprConditionalFilter
CREATE TABLE test_1_n3 (member DECIMAL , attr DECIMAL) STORED AS ORC;

INSERT INTO test_1_n3 VALUES (3.0,1.0),(2.0,2.0),(1.0,3.0);
--for length=3
EXPLAIN VECTORIZATION DETAIL
SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE attr+2.0 END FROM test_1_n3;

SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE attr+2.0 END FROM test_1_n3;

--for length=2 and the expr2 is null
EXPLAIN VECTORIZATION DETAIL
SELECT CASE WHEN member =1.0 THEN 1.0 ELSE attr+2.0 END FROM test_1_n3;

SELECT CASE WHEN member =1.0 THEN 1.0 ELSE attr+2.0 END FROM test_1_n3;

--for length=2 and the expr3 is null
EXPLAIN VECTORIZATION DETAIL
SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE 2.0 END FROM test_1_n3;

SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE 2.0 END FROM test_1_n3;

-- add test for IF**.java call IfExprConditionalFilter
CREATE TABLE test_2_n3 (member BIGINT, attr BIGINT) STORED AS ORC;

INSERT INTO test_2_n3 VALUES (3,1),(2,2),(1,3);

--for length=3
EXPLAIN VECTORIZATION DETAIL
SELECT CASE WHEN member=1 THEN attr+1 else attr+2 END FROM test_2_n3;

SELECT CASE WHEN member=1 THEN attr+1 else attr+2 END FROM test_2_n3;

--for length=2 and the detail2 is null
EXPLAIN VECTORIZATION DETAIL
SELECT CASE WHEN member=1 THEN null else attr+2 END FROM test_2_n3;

SELECT CASE WHEN member=1 THEN null else attr+2 END FROM test_2_n3;

--for length=2 and the detail3 is null
EXPLAIN VECTORIZATION DETAIL
SELECT CASE WHEN member=1 THEN attr+1 else null END FROM test_2_n3;

SELECT CASE WHEN member=1 THEN attr+1 else null END FROM test_2_n3;


select count(*), sum(a.ceven)
from (
select
  case when cint % 2 = 0 then 1 else 0 end as ceven
from alltypesorc) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 0) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 0 AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 1) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then 1 else 0 end) = 1 AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then 1 else 0 end) as ceven
from alltypesorc
where cint is null) a;


select count(*), sum(a.ceven)
from (
select
  case when cint % 2 = 0 then cint else 0 end as ceven
from alltypesorc) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = 0) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = 0 AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = cint) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where (case when cint % 2 = 0 then cint else 0 end) = cint AND cint is NOT NULL) a;

select count(*)
from (
select
  (case when cint % 2 = 0 then cint else 0 end) as ceven
from alltypesorc
where cint is null) a;


