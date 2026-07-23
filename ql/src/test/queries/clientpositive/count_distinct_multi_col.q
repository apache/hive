create table t (col1 string, col2 int, col3 int, col4 date);

insert into t values
  ('a', 1,  2,  '2022-01-01'),
  ('a', 1,  2,  '2022-01-01'),
  ('a', 1,  2,  '2022-01-02'),
  ('a', 1,  22, '2022-01-02'),
  ('a', 11, 2,  '2022-01-01'),
  ('a', 11, 2,  '2022-01-02');

-- single-column: HiveExpandDistinctAggregatesRule matched -> two HiveAggregate operators in plan
explain cbo
SELECT col1, col2, COUNT(DISTINCT col3) AS cnt
FROM t
GROUP BY col1, col2;

SELECT col1, col2, COUNT(DISTINCT col3) AS cnt
FROM t
GROUP BY col1, col2
ORDER BY col1, col2;

-- multi-column: HiveExpandDistinctAggregatesRule !matched -> single HiveAggregate operator
explain cbo
SELECT col1, col2, COUNT(DISTINCT col3, col4) AS cnt
FROM t
GROUP BY col1, col2;

SELECT col1, col2, COUNT(DISTINCT col3, col4) AS cnt
FROM t
GROUP BY col1, col2
ORDER BY col1, col2;

-- 3-column: HiveExpandDistinctAggregatesRule !matched -> single HiveAggregate operator
explain cbo
SELECT col1, COUNT(DISTINCT col2, col3, col4) AS cnt
FROM t
GROUP BY col1;

SELECT col1, COUNT(DISTINCT col2, col3, col4) AS cnt
FROM t
GROUP BY col1;

-- multi-column distinct alongside a non-distinct aggregate: single HiveAggregate operator
explain cbo
SELECT col1, col2, COUNT(DISTINCT col3, col4) AS cnt, SUM(col3) AS s
FROM t
GROUP BY col1, col2;

SELECT col1, col2, COUNT(DISTINCT col3, col4) AS cnt, SUM(col3) AS s
FROM t
GROUP BY col1, col2
ORDER BY col1, col2;

-- multiple COUNT DISTINCT with different single-column arg: uses grouping-sets
explain cbo
SELECT col1, COUNT(DISTINCT col3) AS c3, COUNT(DISTINCT col2) AS c2
FROM t
GROUP BY col1;

SELECT col1, COUNT(DISTINCT col3) AS c3, COUNT(DISTINCT col2) AS c2
FROM t
GROUP BY col1;

-- 2 COUNT DISTINCTs each with multiple columns: uses grouping-sets path
explain cbo
SELECT col1, COUNT(DISTINCT col2, col3) AS c1, COUNT(DISTINCT col3, col4) AS c2
FROM t
GROUP BY col1;

SELECT col1, COUNT(DISTINCT col2, col3) AS c1, COUNT(DISTINCT col3, col4) AS c2
FROM t
GROUP BY col1;

-- CTAS
create table t_ctas as
SELECT col1, col2, COUNT(DISTINCT col3, col4) AS cnt
FROM t
GROUP BY col1, col2;

select * from t_ctas ORDER BY col1, col2;
