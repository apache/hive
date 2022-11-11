--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION acosh;
DESCRIBE FUNCTION EXTENDED acosh;

SELECT acosh(null)
FROM src tablesample (1 rows);

SELECT acosh(0.98), acosh(1.57), acosh(-0.5)
FROM src tablesample (1 rows);
