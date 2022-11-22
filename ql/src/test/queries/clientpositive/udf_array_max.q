--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION array_max;
DESCRIBE FUNCTION EXTENDED array_max;

-- evalutes function for array of primitives
SELECT array_max(array(1, 2, 3, null)) FROM src tablesample (1 rows);

SELECT array_max(array()) FROM src tablesample (1 rows);

SELECT array_max(array(null)) FROM src tablesample (1 rows);

SELECT array_max(array(1.12, 2.23, 3.34, null)) FROM src tablesample (1 rows);

SELECT array_max(array(1.1234567890, 2.234567890, 3.34567890, null)) FROM src tablesample (1 rows);

SELECT array_max(array(11234567890, 2234567890, 334567890, null)) FROM src tablesample (1 rows);