--! qt:dataset:src1
--! qt:dataset:src
--! qt:dataset:cbo_t1
set hive.strict.checks.cartesian.product=false;

-- Conjunction with pred on multiple inputs and single inputs
EXPLAIN
SELECT *
FROM src1 JOIN src
ON (src1.key=src.key
  AND src1.value between 100 and 102
  AND src.value between 100 and 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

SELECT *
FROM src1 JOIN src
ON (src1.key=src.key
  AND src1.value between 100 and 102
  AND src.value between 100 and 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

-- Conjunction with pred on multiple inputs and none
EXPLAIN
SELECT *
FROM src1 JOIN src
ON (src1.key=src.key AND true)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

SELECT *
FROM src1 JOIN src
ON (src1.key=src.key AND true)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

-- Conjunction with pred on single inputs and none
EXPLAIN
SELECT *
FROM src1 JOIN src
ON (src1.value between 100 and 102
  AND src.value between 100 and 102
  AND true)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

SELECT *
FROM src1 JOIN src
ON (src1.value between 100 and 102
  AND src.value between 100 and 102
  AND true)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

-- Disjunction with pred on multiple inputs and single inputs
EXPLAIN
SELECT *
FROM src1 JOIN src
ON (src1.key=src.key
  OR src1.value between 100 and 102
  OR src.value between 100 and 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

SELECT *
FROM src1 JOIN src
ON (src1.key=src.key
  OR src1.value between 100 and 102
  OR src.value between 100 and 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

-- Conjunction with multiple inputs on one side
EXPLAIN
SELECT *
FROM src1 JOIN src
ON (src1.key+src.key >= 100
  AND src1.key+src.key <= 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

SELECT *
FROM src1 JOIN src
ON (src1.key+src.key >= 100
  AND src1.key+src.key <= 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

-- Disjunction with multiple inputs on one side
EXPLAIN
SELECT *
FROM src1 JOIN src
ON (src1.key+src.key >= 100
  OR src1.key+src.key <= 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

SELECT *
FROM src1 JOIN src
ON (src1.key+src.key >= 100
  OR src1.key+src.key <= 102)
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

-- Function with multiple inputs on one side
EXPLAIN
SELECT *
FROM src1 JOIN src
ON ((src1.key,src.key) IN ((100,100),(101,101),(102,102)))
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

SELECT *
FROM src1 JOIN src
ON ((src1.key,src.key) IN ((100,100),(101,101),(102,102)))
ORDER BY src1.key, src1.value, src.key, src.value
LIMIT 10;

-- Chained 1
EXPLAIN
SELECT *
FROM src
JOIN src1 a ON (a.key+src.key >= 100)
LEFT OUTER JOIN src1 b ON (b.key = src.key)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

SELECT *
FROM src
JOIN src1 a ON (a.key+src.key >= 100)
LEFT OUTER JOIN src1 b ON (b.key = src.key)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

-- Chained 2
EXPLAIN
SELECT *
FROM src
LEFT OUTER JOIN src1 a ON (a.key = src.key)
JOIN src1 b ON (b.key+src.key<= 102)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

SELECT *
FROM src
LEFT OUTER JOIN src1 a ON (a.key = src.key)
JOIN src1 b ON (b.key+src.key<= 102)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

-- Chained 3
EXPLAIN
SELECT *
FROM src
JOIN src1 a ON (a.key+src.key >= 100)
RIGHT OUTER JOIN src1 b ON (b.key = src.key)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

SELECT *
FROM src
JOIN src1 a ON (a.key+src.key >= 100)
RIGHT OUTER JOIN src1 b ON (b.key = src.key)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

-- Chained 4
EXPLAIN
SELECT *
FROM src
RIGHT OUTER JOIN src1 a ON (a.key = src.key)
JOIN src1 b ON (b.key+src.key<= 102)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

SELECT *
FROM src
RIGHT OUTER JOIN src1 a ON (a.key = src.key)
JOIN src1 b ON (b.key+src.key<= 102)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

-- Chained 5
EXPLAIN
SELECT *
FROM src
JOIN src1 a ON (a.key+src.key >= 100)
FULL OUTER JOIN src1 b ON (b.key = src.key)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

SELECT *
FROM src
JOIN src1 a ON (a.key+src.key >= 100)
FULL OUTER JOIN src1 b ON (b.key = src.key)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

-- Chained 6
EXPLAIN
SELECT *
FROM src
FULL OUTER JOIN src1 a ON (a.key = src.key)
JOIN src1 b ON (b.key+src.key<= 102)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

SELECT *
FROM src
FULL OUTER JOIN src1 a ON (a.key = src.key)
JOIN src1 b ON (b.key+src.key<= 102)
ORDER BY src.key, src.value, a.key, a.value, b.key, b.value
LIMIT 10;

-- Right outer join with multiple inner joins and mixed conditions
EXPLAIN
SELECT *
FROM cbo_t1 t1
RIGHT OUTER JOIN cbo_t1 t2 ON (t2.key = t1.key)
JOIN cbo_t1 t3 ON (t3.key = t2.key or t3.value = t2.value and t2.c_int = t1.c_int)
JOIN cbo_t1 t4 ON (t4.key = t2.key or  t2.c_float = t4.c_float and t4.value = t2.value)
JOIN cbo_t1 t5 ON (t5.key = t2.key or  t2.c_boolean = t4.c_boolean and t5.c_int = 42)
ORDER BY t1.key, t1.value, t1.c_int, t1.c_float, t1.c_boolean, t2.key, t2.value, t2.c_int, t2.c_float, t2.c_boolean, t3.key, t3.value, t3.c_int, t3.c_float, t3.c_boolean, t4.key, t4.value, t4.c_int, t4.c_float, t4.c_boolean, t5.key, t5.value, t5.c_int, t5.c_float, t5.c_boolean
LIMIT 10;

SELECT *
FROM cbo_t1 t1
RIGHT OUTER JOIN cbo_t1 t2 ON (t2.key = t1.key)
JOIN cbo_t1 t3 ON (t3.key = t2.key or t3.value = t2.value and t2.c_int = t1.c_int)
JOIN cbo_t1 t4 ON (t4.key = t2.key or  t2.c_float = t4.c_float and t4.value = t2.value)
JOIN cbo_t1 t5 ON (t5.key = t2.key or  t2.c_boolean = t4.c_boolean and t5.c_int = 42)
ORDER BY t1.key, t1.value, t1.c_int, t1.c_float, t1.c_boolean, t2.key, t2.value, t2.c_int, t2.c_float, t2.c_boolean, t3.key, t3.value, t3.c_int, t3.c_float, t3.c_boolean, t4.key, t4.value, t4.c_int, t4.c_float, t4.c_boolean, t5.key, t5.value, t5.c_int, t5.c_float, t5.c_boolean
LIMIT 10;
