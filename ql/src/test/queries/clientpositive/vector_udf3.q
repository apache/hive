ADD JAR ivy://org.apache.hive.hive-it-custom-udfs:udf-vectorized-badexample:+;

CREATE TEMPORARY FUNCTION rot13 as 'hive.it.custom.udfs.GenericUDFRot13';

set hive.vectorized.execution.enabled=true;

EXPLAIN SELECT rot13(cstring1) from alltypesorc;

SELECT cstring1, rot13(cstring1) from alltypesorc order by cstring1 desc limit 10;

set hive.vectorized.execution.enabled=false;

SELECT cstring1, rot13(cstring1) from alltypesorc order by cstring1 desc limit 10;
