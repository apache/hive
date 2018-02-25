set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled = true
;
explain vectorization expression
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
explain vectorization expression
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
explain vectorization expression
select
  sum(case when cint % 2 = 0 then 1 else 0 end) as ceven,
  sum(case when cint % 2 = 1 then 1 else 0 end) as codd
from alltypesorc;
select
  sum(case when cint % 2 = 0 then 1 else 0 end) as ceven,
  sum(case when cint % 2 = 1 then 1 else 0 end) as codd
from alltypesorc;
explain vectorization expression
select
  sum(case when cint % 2 = 0 then cint else 0 end) as ceven,
  sum(case when cint % 2 = 1 then cint else 0 end) as codd
from alltypesorc;
select
  sum(case when cint % 2 = 0 then cint else 0 end) as ceven,
  sum(case when cint % 2 = 1 then cint else 0 end) as codd
from alltypesorc;

-- add test for VectorUDFAdaptor call IfExprConditionalFilter
CREATE TABLE test_1 (member DECIMAL , attr DECIMAL) STORED AS ORC;

INSERT INTO test_1 VALUES (3.0,1.0),(2.0,2.0),(1.0,3.0);
--for length=3
EXPLAIN VECTORIZATION EXPRESSION
SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE attr+2.0 END FROM test_1;

SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE attr+2.0 END FROM test_1;

--for length=2 and the expr2 is null
EXPLAIN VECTORIZATION EXPRESSION
SELECT CASE WHEN member =1.0 THEN 1.0 ELSE attr+2.0 END FROM test_1;

SELECT CASE WHEN member =1.0 THEN 1.0 ELSE attr+2.0 END FROM test_1;

--for length=2 and the expr3 is null
EXPLAIN VECTORIZATION EXPRESSION
SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE 2.0 END FROM test_1;

SELECT CASE WHEN member =1.0 THEN attr+1.0 ELSE 2.0 END FROM test_1;

-- add test for IF**.java call IfExprConditionalFilter
CREATE TABLE test_2 (member BIGINT, attr BIGINT) STORED AS ORC;

INSERT INTO test_2 VALUES (3,1),(2,2),(1,3);

--for length=3
EXPLAIN VECTORIZATION EXPRESSION
SELECT CASE WHEN member=1 THEN attr+1 else attr+2 END FROM test_2;

SELECT CASE WHEN member=1 THEN attr+1 else attr+2 END FROM test_2;

--for length=2 and the expression2 is null
EXPLAIN VECTORIZATION EXPRESSION
SELECT CASE WHEN member=1 THEN null else attr+2 END FROM test_2;

SELECT CASE WHEN member=1 THEN null else attr+2 END FROM test_2;

--for length=2 and the expression3 is null
EXPLAIN VECTORIZATION EXPRESSION
SELECT CASE WHEN member=1 THEN attr+1 else null END FROM test_2;

SELECT CASE WHEN member=1 THEN attr+1 else null END FROM test_2;