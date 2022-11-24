--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION array_distinct;
DESCRIBE FUNCTION EXTENDED array_distinct;

-- evalutes function for array of primitives
SELECT array_distinct(array(1, 2, 3, null,3,4)) FROM src tablesample (1 rows);

SELECT array_distinct(array()) FROM src tablesample (1 rows);

SELECT array_distinct(array(null)) FROM src tablesample (1 rows);

SELECT array_distinct(array(1.12, 2.23, 3.34, null,1.11,1.12,2.9)) FROM src tablesample (1 rows);

SELECT array_distinct(array(1.1234567890, 2.234567890, 3.34567890, null, 3.3456789, 2.234567,1.1234567890)) FROM src tablesample (1 rows);

SELECT array_distinct(array(11234567890, 2234567890, 334567890, null, 11234567890, 2234567890, 334567890, null)) FROM src tablesample (1 rows);

SELECT array_distinct(array(array("a","b","c","d"),array("a","b","c","d"),array("a","b","c","d","e"),null,array("e","a","b","c","d"))) FROM src tablesample (1 rows);