add jar ../build/contrib/hive_contrib.jar;
CREATE TEMPORARY FUNCTION example_min_n AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMinN';

EXPLAIN
SELECT example_min_n(substr(value,5),10),
       example_min_n(IF(substr(value,5) < 250, NULL, substr(value,5)),10)
FROM src;

SELECT example_min_n(substr(value,5),10),
       example_min_n(IF(substr(value,5) < 250, NULL, substr(value,5)),10)
FROM src;

DROP TEMPORARY FUNCTION example_min_n;
