--! qt:dataset:src

set hive.optimize.shared.work.max.siblings=3;

EXPLAIN
SELECT count(*), SUM(t1.num), SUM(t2.num), SUM(t3.num), SUM(t4.num), SUM(t5.num), SUM(t6.num), SUM(t7.num)
FROM (SELECT key, count(*) AS num FROM src WHERE key LIKE '%0%' GROUP BY key) t0
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%1%' GROUP BY key) t1 ON t0.key = t1.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%2%' GROUP BY key) t2 ON t0.key = t2.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%3%' GROUP BY key) t3 ON t0.key = t3.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%4%' GROUP BY key) t4 ON t0.key = t4.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%5%' GROUP BY key) t5 ON t0.key = t5.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%6%' GROUP BY key) t6 ON t0.key = t6.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%7%' GROUP BY key) t7 ON t0.key = t7.key;

SELECT count(*), SUM(t1.num), SUM(t2.num), SUM(t3.num), SUM(t4.num), SUM(t5.num), SUM(t6.num), SUM(t7.num)
FROM (SELECT key, count(*) AS num FROM src WHERE key LIKE '%0%' GROUP BY key) t0
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%1%' GROUP BY key) t1 ON t0.key = t1.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%2%' GROUP BY key) t2 ON t0.key = t2.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%3%' GROUP BY key) t3 ON t0.key = t3.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%4%' GROUP BY key) t4 ON t0.key = t4.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%5%' GROUP BY key) t5 ON t0.key = t5.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%6%' GROUP BY key) t6 ON t0.key = t6.key
LEFT OUTER JOIN (SELECT key, count(*) AS num FROM src WHERE key LIKE '%7%' GROUP BY key) t7 ON t0.key = t7.key;
