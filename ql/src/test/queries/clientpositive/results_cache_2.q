--! qt:dataset:src

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;
set hive.fetch.task.conversion=more;

-- Test 1: fetch task
explain
select key, value from src where key=0;
select key, value from src where key=0;

set test.comment=Query only requires fetch task - should not use results cache;
set test.comment;
explain
select key, value from src where key=0;


-- Test 2: deterministic function should use cache.
select c1, count(*)
from (select sign(value) c1, value from src where key < 10) q
group by c1;

set test.comment=This query should use the cache;
set test.comment;
explain
select c1, count(*)
from (select sign(value) c1, value from src where key < 10) q
group by c1;

-- Test 3: non-deterministic functions should not be cached
-- Set current timestamp config to get repeatable result.
set hive.test.currenttimestamp=2012-01-01 01:02:03;

select c1, count(*)
from (select current_timestamp c1, value from src where key < 10) q
group by c1;

set test.comment=Queries using non-deterministic functions should not use results cache;
set test.comment;
explain
select c1, count(*)
from (select current_timestamp c1, value from src where key < 10) q
group by c1;

-- Test 4: cache disabled for explain analyze
set test.comment=EXPLAIN ANALYZE should not use the cache. This query just previously used the cache in Test 2;
set test.comment;
explain analyze
select c1, count(*)
from (select sign(value) c1, value from src where key < 10) q
group by c1;
