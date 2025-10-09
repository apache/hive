--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION sinh;
DESCRIBE FUNCTION EXTENDED sinh;

SELECT sinh(null)
FROM src tablesample (1 rows);

SELECT sinh(0.98), sinh(1.57), sinh(-0.5)
FROM src tablesample (1 rows);
