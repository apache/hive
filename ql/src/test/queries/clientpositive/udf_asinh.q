--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION asinh;
DESCRIBE FUNCTION EXTENDED asinh;

SELECT asinh(null)
FROM src tablesample (1 rows);

SELECT asinh(0.98), asinh(1.57), asinh(-0.5)
FROM src tablesample (1 rows);
