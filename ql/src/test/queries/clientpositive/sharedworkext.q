--! qt:dataset:src
EXPLAIN
SELECT a.key FROM
(
  SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
  GROUP BY a1.key, a2.value
) a
JOIN
(
  SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
  GROUP BY a1.key, a2.value
) b
ON a.key = b.key;

SELECT a.key FROM
(
  SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
  GROUP BY a1.key, a2.value
) a
JOIN
(
  SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
  GROUP BY a1.key, a2.value
) b
ON a.key = b.key;

EXPLAIN
SELECT a.key FROM
(
  SELECT rank() OVER (ORDER BY key) AS key FROM
    (SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
    GROUP BY a1.key, a2.value) a
) a
JOIN
(
  SELECT rank() OVER (ORDER BY key) AS key FROM
    (SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
    GROUP BY a1.key, a2.value) b
) b
ON a.key = b.key;

SELECT a.key FROM
(
  SELECT rank() OVER (ORDER BY key) AS key FROM
    (SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
    GROUP BY a1.key, a2.value) a
) a
JOIN
(
  SELECT rank() OVER (ORDER BY key) AS key FROM
    (SELECT a1.key, a2.value FROM src a1 JOIN src a2 ON (a1.key = a2.key)
    GROUP BY a1.key, a2.value) b
) b
ON a.key = b.key;
