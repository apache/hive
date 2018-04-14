--! qt:dataset:src
-- VALUES -> array(struct(),struct())
-- TABLE -> LATERAL VIEW INLINE

CREATE TABLE mytbl_n1 AS
SELECT key, value
FROM src
ORDER BY key
LIMIT 5;

EXPLAIN
INSERT INTO mytbl_n1(key,value)
SELECT a,b as c FROM TABLE(VALUES(1,2),(3,4)) AS vc(a,b)
WHERE b = 9;

INSERT INTO mytbl_n1(key,value)
SELECT a,b as c FROM TABLE(VALUES(1,2),(3,4)) AS vc(a,b)
WHERE b = 9;

EXPLAIN
SELECT tf.col1, tf.col2, tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t
  LATERAL VIEW
  INLINE(array(struct('A', 10, t.key),struct('B', 20, t.key))) tf AS col1, col2, col3;

SELECT tf.col1, tf.col2, tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t
  LATERAL VIEW
  INLINE(array(struct('A', 10, t.key),struct('B', 20, t.key))) tf AS col1, col2, col3;

EXPLAIN
SELECT INLINE(array(struct('A', 10, 30),struct('B', 20, 30))) AS (col1, col2, col3);

SELECT INLINE(array(struct('A', 10, 30),struct('B', 20, 30))) AS (col1, col2, col3);

EXPLAIN
SELECT tf.col1, tf.col2, tf.col3
FROM
  TABLE(VALUES('A', 10, 30.0),('B', 20, 30.0)) AS tf(col1, col2, col3);

SELECT tf.col1, tf.col2, tf.col3
FROM
  TABLE(VALUES('A', 10, 30),('B', 20, 30)) AS tf(col1, col2, col3);

-- CROSS PRODUCT (CANNOT BE EXPRESSED WITH LVJ)
EXPLAIN
SELECT tf.col1, tf.col2, tf.col3
FROM
  TABLE(VALUES('A', 10, 30),('B', 20, 30)) AS tf(col1, col2, col3),
  (SELECT key, value FROM mytbl_n1) t;

SELECT tf.col1, tf.col2, tf.col3
FROM
  TABLE(VALUES('A', 10, 30),('B', 20, 30)) AS tf(col1, col2, col3),
  (SELECT key, value FROM mytbl_n1) t;

-- CROSS PRODUCT (FIRST CANNOT BE EXPRESSED WITH LVJ, SECOND CAN
-- BUT IT IS NOT NEEDED)
EXPLAIN
SELECT tf.col1, tf.col2, tf.col3
FROM
  TABLE(VALUES('A', 10, 30),('B', 20, 30)) AS tf(col1, col2, col3),
  TABLE(VALUES('A', 10),('B', 20)) AS tf2(col1, col2);

SELECT tf.col1, tf.col2, tf.col3
FROM
  TABLE(VALUES('A', 10, 30),('B', 20, 30)) AS tf(col1, col2, col3),
  TABLE(VALUES('A', 10),('B', 20)) AS tf2(col1, col2);

-- CROSS PRODUCT (CAN BE EXPRESSED WITH LVJ BUT IT IS NOT NEEDED)
EXPLAIN
SELECT tf.col1, tf.col2, tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t,
  TABLE(VALUES('A', 10, 30),('B', 20, 30)) AS tf(col1, col2, col3);

SELECT tf.col1, tf.col2, tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t,
  TABLE(VALUES('A', 10, 30),('B', 20, 30)) AS tf(col1, col2, col3);

-- LVJ (CORRELATED). LATERAL COULD BE OPTIONAL, BUT IF WE MAKE IT
-- MANDATORY, IT HELPS US DISTINGUISHING FROM PREVIOUS CASE
EXPLAIN
SELECT tf.col1, tf.col2, tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.key),('B', 20, t.key)) AS tf(col1, col2, col3);

SELECT tf.col1, tf.col2, tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.key),('B', 20, t.key)) AS tf(col1, col2, col3);

EXPLAIN
SELECT t.key
FROM
  (SELECT key, value FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.key),('B', 20, t.key)) AS tf;

SELECT t.key
FROM
  (SELECT key, value FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.key),('B', 20, t.key)) AS tf;

EXPLAIN
SELECT tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.key),('B', 20, t.key)) AS tf(col1, col2, col3);

SELECT tf.col3
FROM
  (SELECT key, value FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.key),('B', 20, t.key)) AS tf(col1, col2, col3);

EXPLAIN
SELECT tf.col3
FROM
  (SELECT row_number() over (order by key desc) as r FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.r),('B', 20, t.r)) AS tf(col1, col2, col3);

SELECT tf.col3
FROM
  (SELECT row_number() over (order by key desc) as r FROM mytbl_n1) t,
  LATERAL TABLE(VALUES('A', 10, t.r),('B', 20, t.r)) AS tf(col1, col2, col3);
