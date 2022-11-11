--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION atanh;
DESCRIBE FUNCTION EXTENDED atanh;

SELECT atanh(null)
FROM src tablesample (1 rows);

SELECT atanh(0.98), atanh(1.57), atanh(-0.5)
FROM src tablesample (1 rows);
