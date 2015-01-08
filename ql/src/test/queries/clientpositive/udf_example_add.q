add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION example_add AS 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd';

EXPLAIN
SELECT example_add(1, 2),
       example_add(1, 2, 3),
       example_add(1, 2, 3, 4),
       example_add(1.1, 2.2),
       example_add(1.1, 2.2, 3.3),
       example_add(1.1, 2.2, 3.3, 4.4),
       example_add(1, 2, 3, 4.4)
FROM src LIMIT 1;

SELECT example_add(1, 2),
       example_add(1, 2, 3),
       example_add(1, 2, 3, 4),
       example_add(1.1, 2.2),
       example_add(1.1, 2.2, 3.3),
       example_add(1.1, 2.2, 3.3, 4.4),
       example_add(1, 2, 3, 4.4)
FROM src LIMIT 1;

DROP TEMPORARY FUNCTION example_add;
