--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION tanh;
DESCRIBE FUNCTION EXTENDED tanh;

SELECT tanh(null)
FROM src tablesample (1 rows);

SELECT tanh(1), tanh(6), tanh(-1.0)
FROM src tablesample (1 rows);
