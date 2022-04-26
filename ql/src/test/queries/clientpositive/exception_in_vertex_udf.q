--! qt:dataset:src


-- exception_in_vertex_udf and exception_in_vertex_udaf examples show typical patterns of usage,
-- they parameterized to not fail, otherwise a clientpositive test is not sufficient,
-- so the corresponding q.out file doesn't contain useful information about the exception being thrown,
-- but itests/qtest/target/tmp/log/hive.log does, you can check every task attempt failure by looking for 'found condition for throwing exception' messages
-- on the other side, clientnegative tests cannot be written at the moment
-- as TestNegativeCliDriver won't utilize TezProcessor, where the actual vertex name, task number, task attempt number
-- is set, this also implies that the udfs below support only tez, and they are not supposed to support any other execution engines
-- (MR is not supported in the future anyway)

-- exception_in_vertex_udf can be typically used on mapper side and
-- needs a column reference for the given mapper as the first parameter, because:
-- 1. basically, it would have been an ugly compiler hack to inject this udf into arbitrary mapper,
--    and it's not worth for a udf used for testing purpuses
-- 2. using these udfs already assumes that the user knows the plan

-- Map 1
EXPLAIN
SELECT exception_in_vertex_udf (src1.value, "Map 1", 0, "*")
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

SELECT exception_in_vertex_udf (src1.value, "Map 1", "*", 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

SELECT exception_in_vertex_udf (src1.value, "Map 1", "0,1,2", 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

SELECT exception_in_vertex_udf (src1.value, "Map 1", "0-2", 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

SELECT exception_in_vertex_udf (src1.value, "Map 1", 0, "0")
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

SELECT exception_in_vertex_udf (src1.value, "Map 1", 0, "0")
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

-- Map 3
EXPLAIN
SELECT exception_in_vertex_udf (src2.value, "Map 3", 0, 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

SELECT exception_in_vertex_udf (src2.value, "Map 3", 0, 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;



-- exception_in_vertex_udaf is typically used on reducer side
-- Reducer 2
EXPLAIN
SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 2", 0, 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 2", "*", 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 2", "0,1,2", 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 2", "0-2", 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 2", 0, "0")
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 2", 0, "0")
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

-- Reducer 3
EXPLAIN
SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 3", 0, 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

SELECT sum(src1.value), exception_in_vertex_udaf ("Reducer 3", 0, 0)
FROM src src1
JOIN src src2 ON (src1.key = src2.key);

-- Map 1
EXPLAIN
SELECT exception_in_vertex_udf (src1.value, "Map 1")
FROM src src1
JOIN src src2 ON (src1.key = src2.key)
LIMIT 1;

-- (src1.value, "Map 1") defaults to (src1.value, "Map 1", "*", "*"), so the query below would fail by design, with an error like:
-- Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: GenericUDFExceptionInVertex:
-- found condition for throwing exception (vertex/task/attempt):current Map 1 / 0 / 1 matches criteria Map 1 / * / *
-- so default parameters can make a vertex fail in any case

--SELECT exception_in_vertex_udf (src1.value, "Map 1")
--FROM src src1
--JOIN src src2 ON (src1.key = src2.key)
--LIMIT 1;