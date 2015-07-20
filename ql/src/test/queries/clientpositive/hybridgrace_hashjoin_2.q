set hive.explain.user=false;
-- Hybrid Grace Hash Join
-- Test n-way join
SELECT 1;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;
set hive.cbo.enable=false;


-- 3-way mapjoin (1 big table, 2 small tables)
SELECT 1;

set hive.mapjoin.hybridgrace.hashtable=false;

EXPLAIN
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key);

SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key);

set hive.mapjoin.hybridgrace.hashtable=true;

EXPLAIN
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key);

SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key);


-- 4-way mapjoin (1 big table, 3 small tables)
SELECT 1;

set hive.mapjoin.hybridgrace.hashtable=false;

EXPLAIN
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN srcpart w ON (x.key = w.key)
JOIN src y ON (y.key = x.key);

SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN srcpart w ON (x.key = w.key)
JOIN src y ON (y.key = x.key);

set hive.mapjoin.hybridgrace.hashtable=true;

EXPLAIN
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN srcpart w ON (x.key = w.key)
JOIN src y ON (y.key = x.key);

SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN srcpart w ON (x.key = w.key)
JOIN src y ON (y.key = x.key);


-- 2 sets of 3-way mapjoin under 2 different tasks
SELECT 1;

set hive.mapjoin.hybridgrace.hashtable=false;

EXPLAIN
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key)
UNION
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.value = z.value)
JOIN src y ON (y.value = x.value);

SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key)
UNION
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.value = z.value)
JOIN src y ON (y.value = x.value);

set hive.mapjoin.hybridgrace.hashtable=true;

EXPLAIN
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key)
UNION
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.value = z.value)
JOIN src y ON (y.value = x.value);

SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.key = z.key)
JOIN src y ON (y.key = x.key)
UNION
SELECT COUNT(*)
FROM src1 x JOIN srcpart z ON (x.value = z.value)
JOIN src y ON (y.value = x.value);


-- A chain of 2 sets of 3-way mapjoin under the same task
SELECT 1;

set hive.mapjoin.hybridgrace.hashtable=false;

EXPLAIN
SELECT COUNT(*)
FROM src1 x
JOIN srcpart z1 ON (x.key = z1.key)
JOIN src y1     ON (x.key = y1.key)
JOIN srcpart z2 ON (x.value = z2.value)
JOIN src y2     ON (x.value = y2.value)
WHERE z1.key < 'zzzzzzzz' AND z2.key < 'zzzzzzzzzz'
 AND y1.value < 'zzzzzzzz' AND y2.value < 'zzzzzzzzzz';

SELECT COUNT(*)
FROM src1 x
JOIN srcpart z1 ON (x.key = z1.key)
JOIN src y1     ON (x.key = y1.key)
JOIN srcpart z2 ON (x.value = z2.value)
JOIN src y2     ON (x.value = y2.value)
WHERE z1.key < 'zzzzzzzz' AND z2.key < 'zzzzzzzzzz'
 AND y1.value < 'zzzzzzzz' AND y2.value < 'zzzzzzzzzz';

set hive.mapjoin.hybridgrace.hashtable=true;

EXPLAIN
SELECT COUNT(*)
FROM src1 x
JOIN srcpart z1 ON (x.key = z1.key)
JOIN src y1     ON (x.key = y1.key)
JOIN srcpart z2 ON (x.value = z2.value)
JOIN src y2     ON (x.value = y2.value)
WHERE z1.key < 'zzzzzzzz' AND z2.key < 'zzzzzzzzzz'
 AND y1.value < 'zzzzzzzz' AND y2.value < 'zzzzzzzzzz';

SELECT COUNT(*)
FROM src1 x
JOIN srcpart z1 ON (x.key = z1.key)
JOIN src y1     ON (x.key = y1.key)
JOIN srcpart z2 ON (x.value = z2.value)
JOIN src y2     ON (x.value = y2.value)
WHERE z1.key < 'zzzzzzzz' AND z2.key < 'zzzzzzzzzz'
 AND y1.value < 'zzzzzzzz' AND y2.value < 'zzzzzzzzzz';


reset hive.cbo.enable;
