--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION array_distinct;
DESCRIBE FUNCTION EXTENDED array_distinct;

-- evalutes function for array of primitives
SELECT array_distinct(array(1, 2, 3, null,3)) FROM src tablesample (1 rows);

SELECT array_distinct(array(1.12, 2.23, 3.34, null,1.12,2.23)) FROM src tablesample (1 rows);

SELECT array_distinct(array(1.1234567890, 2.234567890, 3.34567890, null, 2.234567890)) FROM src tablesample (1 rows);

SELECT array_distinct(array(11234567890, 2234567890, 334567890, null, 2234567890)) FROM src tablesample (1 rows);

SELECT array_distinct(array()) FROM src tablesample (1 rows);

SELECT array_distinct(array('b', 'd', 'd', 'a')) FROM src tablesample (1 rows);

SELECT array_distinct(array(array('b', 'd', 'c', 'a'),array('b', 'd', 'c', 'a'),array('a', 'b', 'c', 'd'))) FROM src tablesample (1 rows);
