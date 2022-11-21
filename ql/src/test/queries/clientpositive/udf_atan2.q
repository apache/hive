--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION atan2;
DESCRIBE FUNCTION EXTENDED atan2;

SELECT atan2(null,null)
FROM src tablesample (1 rows);

SELECT atan2(1,null)
FROM src tablesample (1 rows);

SELECT atan2(null,1)
FROM src tablesample (1 rows);

SELECT atan2(1.57,2), atan2(-0.5,-3), atan2(-0.5,3), atan2(0.5,-3)
FROM src tablesample (1 rows);
