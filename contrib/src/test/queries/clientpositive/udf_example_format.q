--! qt:dataset:src
add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION example_format AS 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleFormat';

EXPLAIN
SELECT example_format("abc"),
       example_format("%1$s", 1.1),
       example_format("%1$s %2$e", 1.1D, 1.2D),
       example_format("%1$x %2$o %3$d", 10, 10, 10)
FROM src LIMIT 1;

SELECT example_format("abc"),
       example_format("%1$s", 1.1),
       example_format("%1$s %2$e", 1.1D, 1.2D),
       example_format("%1$x %2$o %3$d", 10, 10, 10)
FROM src LIMIT 1;

DROP TEMPORARY FUNCTION example_format;
