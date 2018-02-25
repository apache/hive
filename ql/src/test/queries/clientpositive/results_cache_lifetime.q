
set hive.query.results.cache.enabled=true;
set hive.query.results.cache.max.entry.lifetime=2;

-- This query used the cache from results_cache_1.q. Load it up.
select count(*) from src a join src b on (a.key = b.key);

-- Make sure we are past the cache entry lifetime
select reflect("java.lang.Thread", 'sleep', cast(2000 as bigint));

set test.comment="Cached entry should be expired - query should not use cache";
set test.comment;
explain
select count(*) from src a join src b on (a.key = b.key);
