EXPLAIN
SELECT stack(1, 'a', 'b', 'c') AS (c1, c2, c3)
UNION ALL
SELECT stack(1, 'd', 'e', 'f') AS (c1, c2, c3);

SELECT stack(1, 'a', 'b', 'c') AS (c1, c2, c3)
UNION ALL
SELECT stack(1, 'd', 'e', 'f') AS (c1, c2, c3);
