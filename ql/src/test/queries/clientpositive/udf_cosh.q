--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION cosh;
DESCRIBE FUNCTION EXTENDED cosh;

SELECT cosh(null)
FROM src tablesample (1 rows);

SELECT cosh(0.98), cosh(1.57), cosh(-0.5)
FROM src tablesample (1 rows);