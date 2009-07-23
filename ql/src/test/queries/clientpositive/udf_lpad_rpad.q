EXPLAIN SELECT
  lpad('hi', 1, '?'),
  lpad('hi', 5, '.'),
  lpad('hi', 6, '123')
FROM src LIMIT 1;

SELECT
  lpad('hi', 1, '?'),
  lpad('hi', 5, '.'),
  lpad('hi', 6, '123')
FROM src LIMIT 1;

EXPLAIN SELECT
  rpad('hi', 1, '?'),
  rpad('hi', 5, '.'),
  rpad('hi', 6, '123')
FROM src LIMIT 1;

SELECT
  rpad('hi', 1, '?'),
  rpad('hi', 5, '.'),
  rpad('hi', 6, '123')
FROM src LIMIT 1;
