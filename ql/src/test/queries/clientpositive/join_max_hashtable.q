set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.hashtable.max.entries=500;
set hive.auto.convert.join.shuffle.max.size=100000;

-- CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);

set hive.auto.convert.join.hashtable.max.entries=300;

-- CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- DO NOT CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);

set hive.auto.convert.join.hashtable.max.entries=10;

-- DO NOT CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- DO NOT CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);

set hive.auto.convert.join.shuffle.max.size=80000;

-- CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key);

-- CONVERT
EXPLAIN
SELECT x.key, x.value
FROM src x JOIN src y ON (x.key = y.key AND x.value = y.value);
