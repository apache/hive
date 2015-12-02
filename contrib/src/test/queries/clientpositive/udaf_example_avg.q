set hive.mapred.mode=nonstrict;
add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION example_avg AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleAvg';

EXPLAIN
SELECT example_avg(substr(value,5)),
       example_avg(IF(substr(value,5) > 250, NULL, substr(value,5)))
FROM src;

SELECT example_avg(substr(value,5)),
       example_avg(IF(substr(value,5) > 250, NULL, substr(value,5)))
FROM src;

DROP TEMPORARY FUNCTION example_avg;
