--! qt:dataset:src
ADD JAR ivy://org.apache.hive.hive-it-custom-udfs:udf-classloader-udf1:+;
ADD JAR ivy://org.apache.hive.hive-it-custom-udfs:udf-classloader-util:+;
ADD JAR ivy://org.apache.hive.hive-it-custom-udfs:udf-classloader-udf2:+;

CREATE TEMPORARY FUNCTION f1 AS 'hive.it.custom.udfs.UDF1';
CREATE TEMPORARY FUNCTION f2 AS 'hive.it.custom.udfs.UDF2';

-- udf-classloader-udf1.jar contains f1 which relies on udf-classloader-util.jar,
-- similiary udf-classloader-udf2.jar contains f2 which also relies on udf-classloader-util.jar.
SELECT f1(*), f2(*) FROM SRC limit 1;

DELETE JAR ivy://org.apache.hive.hive-it-custom-udfs:udf-classloader-udf2:+;
SELECT f1(*) FROM SRC limit 1;

ADD JAR ivy://org.apache.hive.hive-it-custom-udfs:udf-classloader-udf2:+;
SELECT f2(*) FROM SRC limit 1;
