ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-custom-udfs/udf-classloader-udf1/${system:hive.version}/udf-classloader-udf1-${system:hive.version}.jar;
ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-custom-udfs/udf-classloader-util/${system:hive.version}/udf-classloader-util-${system:hive.version}.jar;
ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-custom-udfs/udf-classloader-udf2/${system:hive.version}/udf-classloader-udf2-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION f1 AS 'hive.it.custom.udfs.UDF1';
CREATE TEMPORARY FUNCTION f2 AS 'hive.it.custom.udfs.UDF2';

-- udf-classloader-udf1.jar contains f1 which relies on udf-classloader-util.jar,
-- similiary udf-classloader-udf2.jar contains f2 which also relies on udf-classloader-util.jar.
SELECT f1(*), f2(*) FROM SRC limit 1;

DELETE JAR ${system:maven.local.repository}/org/apache/hive/hive-it-custom-udfs/udf-classloader-udf2/${system:hive.version}/udf-classloader-udf2-${system:hive.version}.jar;
SELECT f1(*) FROM SRC limit 1;

ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-custom-udfs/udf-classloader-udf2/${system:hive.version}/udf-classloader-udf2-${system:hive.version}.jar;
SELECT f2(*) FROM SRC limit 1;
