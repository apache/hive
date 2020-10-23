--! qt:dataset:src
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.hashtable.max.entries=500;
set hive.auto.convert.join.shuffle.max.size=200000;

-- CONVERT (BROADCAST MJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- CONVERT (BROADCAST MJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);

set hive.auto.convert.join.hashtable.max.entries=400;

-- CONVERT (BROADCAST MJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- DO NOT CONVERT (DPHJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);

set hive.auto.convert.join.hashtable.max.entries=10;

-- DO NOT CONVERT (DPHJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- DO NOT CONVERT (DPHJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);

set hive.auto.convert.join.shuffle.max.size=80000;

-- DO NOT CONVERT (DPHJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- DO NOT CONVERT (DPHJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);

set hive.auto.convert.join.shuffle.max.size=1000;

-- DO NOT CONVERT (SMJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- NO DPHJ Shuffle limit
set hive.auto.convert.join.shuffle.max.size=-1;

-- DO NOT CONVERT (DPHJ)
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);
