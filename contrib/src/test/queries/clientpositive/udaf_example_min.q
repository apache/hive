add jar ../build/contrib/hive_contrib.jar;

CREATE TEMPORARY FUNCTION example_min AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMin';

DESCRIBE FUNCTION EXTENDED example_min;

EXPLAIN
SELECT example_min(substr(value,5)),
       example_min(IF(substr(value,5) > 250, NULL, substr(value,5)))
FROM src;

SELECT example_min(substr(value,5)),
       example_min(IF(substr(value,5) > 250, NULL, substr(value,5)))
FROM src;

DROP TEMPORARY FUNCTION example_min;
