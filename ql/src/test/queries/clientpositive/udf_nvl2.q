set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION nvl2;
DESCRIBE FUNCTION EXTENDED nvl2;

EXPLAIN
SELECT NVL2( 1 , 1, 0 ) AS COL1,
       NVL2( NULL, 1, 0 ) AS COL2
FROM src tablesample (1 rows);

SELECT NVL2( 1 , 1, 0 ) AS COL1,
       NVL2( NULL, 1, 0 ) AS COL2
FROM src tablesample (1 rows);