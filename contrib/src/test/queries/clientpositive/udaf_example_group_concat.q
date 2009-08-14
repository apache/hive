add jar ../build/contrib/hive_contrib.jar;

CREATE TEMPORARY FUNCTION example_group_concat AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleGroupConcat';

EXPLAIN
SELECT substr(value,5,1), example_group_concat("(", key, ":", value, ")")
FROM src
GROUP BY substr(value,5,1);

SELECT substr(value,5,1), example_group_concat("(", key, ":", value, ")")
FROM src
GROUP BY substr(value,5,1);


DROP TEMPORARY FUNCTION example_group_concat;
