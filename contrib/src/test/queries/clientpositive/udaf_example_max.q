--! qt:dataset:src
set hive.mapred.mode=nonstrict;
add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION example_max AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax';

DESCRIBE FUNCTION EXTENDED example_max;

EXPLAIN
SELECT example_max(substr(value,5)),
       example_max(IF(substr(value,5) > 250, NULL, substr(value,5)))
FROM src;

SELECT example_max(substr(value,5)),
       example_max(IF(substr(value,5) > 250, NULL, substr(value,5)))
FROM src;

DROP TEMPORARY FUNCTION example_max;
