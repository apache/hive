--! qt:dataset:src

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;

-- Allow results cache to hold entries up to 125 bytes
-- The single row queries are small enough to fit in the cache (103 bytes)
-- But the cache is only large enough to hold up to 2 entries at that size.
-- This may need to be adjusted if the sizes below change
set hive.query.results.cache.max.size=250;
set hive.query.results.cache.max.entry.size=125;


select key, count(*) from src where key = 0 group by key;
set test.comment=Q1 should be cached;
set test.comment;
explain
select key, count(*) from src where key = 0 group by key;


select key, count(*) from src where key = 2 group by key;
set test.comment=Q2 should now be cached;
set test.comment;
explain
select key, count(*) from src where key = 2 group by key;

set test.comment=Q1 should still be cached;
set test.comment;
explain
select key, count(*) from src where key = 0 group by key;

-- Add another query to the cache. Cache not large enough to hold all 3 queries.
-- Due to LRU (Q1 last looked up), Q2 should no longer be in the cache.
select key, count(*) from src where key = 4 group by key;
set test.comment=Q3 should now be cached;
set test.comment;
explain
select key, count(*) from src where key = 4 group by key;

set test.comment=Q1 should still be cached;
set test.comment;
explain
select key, count(*) from src where key = 0 group by key;

set test.comment=Q2 should no longer be in the cache;
set test.comment;
explain
select key, count(*) from src where key = 2 group by key;

-- Query should not be cached because it exceeds the max entry size (183 bytes).
select key, count(*) from src where key < 10 group by key;
set test.comment=Q4 is too large to be cached;
explain
select key, count(*) from src where key < 10 group by key;
