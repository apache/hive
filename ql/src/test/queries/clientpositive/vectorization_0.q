--! qt:dataset:srcbucket
--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

-- Use ORDER BY clauses to generate 2 stages.
EXPLAIN VECTORIZATION DETAIL
SELECT MIN(ctinyint) as c1,
       MAX(ctinyint),
       COUNT(ctinyint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

SELECT MIN(ctinyint) as c1,
       MAX(ctinyint),
       COUNT(ctinyint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION DETAIL
SELECT SUM(ctinyint) as c1
FROM   alltypesorc
ORDER BY c1;

SELECT SUM(ctinyint) as c1
FROM   alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION 
SELECT
  avg(ctinyint) as c1,
  variance(ctinyint),
  var_pop(ctinyint),
  var_samp(ctinyint),
  std(ctinyint),
  stddev(ctinyint),
  stddev_pop(ctinyint),
  stddev_samp(ctinyint)
FROM alltypesorc
ORDER BY c1;

SELECT
  avg(ctinyint) as c1,
  variance(ctinyint),
  var_pop(ctinyint),
  var_samp(ctinyint),
  std(ctinyint),
  stddev(ctinyint),
  stddev_pop(ctinyint),
  stddev_samp(ctinyint)
FROM alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION DETAIL
SELECT MIN(cbigint) as c1,
       MAX(cbigint),
       COUNT(cbigint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

SELECT MIN(cbigint) as c1,
       MAX(cbigint),
       COUNT(cbigint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION DETAIL
SELECT SUM(cbigint) as c1
FROM   alltypesorc
ORDER BY c1;

SELECT SUM(cbigint) as c1
FROM   alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION 
SELECT
  avg(cbigint) as c1,
  variance(cbigint),
  var_pop(cbigint),
  var_samp(cbigint),
  std(cbigint),
  stddev(cbigint),
  stddev_pop(cbigint),
  stddev_samp(cbigint)
FROM alltypesorc
ORDER BY c1;

SELECT
  avg(cbigint) as c1,
  variance(cbigint),
  var_pop(cbigint),
  var_samp(cbigint),
  std(cbigint),
  stddev(cbigint),
  stddev_pop(cbigint),
  stddev_samp(cbigint)
FROM alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION DETAIL
SELECT MIN(cfloat) as c1,
       MAX(cfloat),
       COUNT(cfloat),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

SELECT MIN(cfloat) as c1,
       MAX(cfloat),
       COUNT(cfloat),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION DETAIL
SELECT SUM(cfloat) as c1
FROM   alltypesorc
ORDER BY c1;

SELECT SUM(cfloat) as c1
FROM   alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION 
SELECT
  avg(cfloat) as c1,
  variance(cfloat),
  var_pop(cfloat),
  var_samp(cfloat),
  std(cfloat),
  stddev(cfloat),
  stddev_pop(cfloat),
  stddev_samp(cfloat)
FROM alltypesorc
ORDER BY c1;

SELECT
  avg(cfloat) as c1,
  variance(cfloat),
  var_pop(cfloat),
  var_samp(cfloat),
  std(cfloat),
  stddev(cfloat),
  stddev_pop(cfloat),
  stddev_samp(cfloat)
FROM alltypesorc
ORDER BY c1;

EXPLAIN VECTORIZATION DETAIL
SELECT AVG(cbigint),
       (-(AVG(cbigint))),
       (-6432 + AVG(cbigint)),
       STDDEV_POP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) + (-6432 + AVG(cbigint))),
       VAR_SAMP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       (-6432 + (-((-6432 + AVG(cbigint))))),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) / (-((-6432 + AVG(cbigint))))),
       COUNT(*),
       SUM(cfloat),
       (VAR_SAMP(cbigint) % STDDEV_POP(cbigint)),
       (-(VAR_SAMP(cbigint))),
       ((-((-6432 + AVG(cbigint)))) * (-(AVG(cbigint)))),
       MIN(ctinyint),
       (-(MIN(ctinyint)))
FROM   alltypesorc
WHERE  (((cstring2 LIKE '%b%')
         OR ((79.553 != cint)
             OR (cbigint < cdouble)))
        OR ((ctinyint >= csmallint)
            AND ((cboolean2 = 1)
                 AND (3569 = ctinyint))));

SELECT AVG(cbigint),
       (-(AVG(cbigint))),
       (-6432 + AVG(cbigint)),
       STDDEV_POP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) + (-6432 + AVG(cbigint))),
       VAR_SAMP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       (-6432 + (-((-6432 + AVG(cbigint))))),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) / (-((-6432 + AVG(cbigint))))),
       COUNT(*),
       SUM(cfloat),
       (VAR_SAMP(cbigint) % STDDEV_POP(cbigint)),
       (-(VAR_SAMP(cbigint))),
       ((-((-6432 + AVG(cbigint)))) * (-(AVG(cbigint)))),
       MIN(ctinyint),
       (-(MIN(ctinyint)))
FROM   alltypesorc
WHERE  (((cstring2 LIKE '%b%')
         OR ((79.553 != cint)
             OR (cbigint < cdouble)))
        OR ((ctinyint >= csmallint)
            AND ((cboolean2 = 1)
                 AND (3569 = ctinyint))));

EXPLAIN extended
select count(*) from alltypesorc
                     where (((cstring1 LIKE 'a%') or ((cstring1 like 'b%') or (cstring1 like 'c%'))) or
                           ((length(cstring1) < 50 ) and ((cstring1 like '%n') and (length(cstring1) > 0))));

select count(*) from alltypesorc
                     where (((cstring1 LIKE 'a%') or ((cstring1 like 'b%') or (cstring1 like 'c%'))) or
                           ((length(cstring1) < 50 ) and ((cstring1 like '%n') and (length(cstring1) > 0))));

set hive.vectorized.execution.enabled=true;
set hive.compute.query.using.stats=false;

select min(ctinyint), max(ctinyint), sum(ctinyint), avg(ctinyint) from alltypesorc;
select min(csmallint), max(csmallint), sum(csmallint), avg(csmallint) from alltypesorc;
select min(cint), max(cint), sum(cint), avg(cint) from alltypesorc;
select min(cbigint), max(cbigint), sum(cbigint), avg(cbigint) from alltypesorc;
select min(cdouble), max(cdouble), sum(cdouble), avg(cdouble) from alltypesorc;
select distinct cstring1 from alltypesorc;
select distinct cstring1, ctinyint from alltypesorc;
select cstring1, max(cbigint) from alltypesorc
                          group by cstring1
                          order by cstring1 desc;

set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.reduce.groupby.enabled=true;
select cstring1, cint, ctinyint from alltypesorc
                           where cstring1 > 'religion';
select cstring1, cint, ctinyint from alltypesorc where cstring1 <> 'religion';

select ctinyint, csmallint, cint, cbigint, cdouble, cdouble, cstring1 from alltypesorc
                           where ctinyint > 0 and csmallint > 0 and cint > 0 and cbigint > 0 and
                                 cfloat > 0.0 and cdouble > 0.0 and cstring1 > 'm';

set hive.optimize.point.lookup=false;
--test to make sure multi and/or expressions are being vectorized
explain extended select * from alltypesorc where
                     (cint=49 and  cfloat=3.5) or
                     (cint=47  and  cfloat=2.09) or
                     (cint=45  and  cfloat=3.02);

set hive.optimize.point.lookup=true;
set hive.optimize.point.lookup.min=1;

explain extended select * from alltypesorc where
                                (cint=49 and  cfloat=3.5) or
                                (cint=47  and  cfloat=2.09) or
                                (cint=45  and  cfloat=3.02);

explain extended select * from alltypesorc where
                     (cint=49 or  cfloat=3.5) and
                     (cint=47 or  cfloat=2.09) and
                     (cint=45 or  cfloat=3.02);

explain extended select count(*),cstring1 from alltypesorc where cstring1='biology'
                                                    or cstring1='history'
                                                    or cstring1='topology' group by cstring1 order by cstring1;


drop table if exists cast_string_to_int_1_n0;
drop table if exists cast_string_to_int_2_n0;

create table cast_string_to_int_1_n0 as select CAST(CAST(key as float) as string),value from srcbucket;
create table cast_string_to_int_2_n0(i int,s string);
insert overwrite table cast_string_to_int_2_n0 select * from cast_string_to_int_1_n0;

--moving ALL_1 system test here
select all key from src;
