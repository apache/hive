--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.limit.pushdown.memory.usage=0.3f;
set hive.optimize.reducededuplication.min.reducer=1;

-- SORT_QUERY_RESULTS

-- push through LOJ

set hive.optimize.limit=true;

select 'positive LOJ';
explain
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) LIMIT 5;
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) LIMIT 5;

select 'negative - order';
explain
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) ORDER BY src1.key LIMIT 5;

select 'negative - group by';
explain
SELECT src1.key, count(src2.value) FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key LIMIT 5;

set hive.optimize.limit=false;

select 'negative - disabled';

explain
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) LIMIT 5;
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) LIMIT 5;