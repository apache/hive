--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
EXPLAIN
SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 10,10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 10,10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 0,10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 1,10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 300,100;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 100 OFFSET 300;

set hive.cbo.returnpath.hiveop=true;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 10,10;

set hive.cbo.returnpath.hiveop=false;
set hive.cbo.enable=false;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 10,10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 0,10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 1,10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 300,100;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key ORDER BY src.key LIMIT 100 OFFSET 300;

