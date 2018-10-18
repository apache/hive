set hive.stats.dbclass=fs;

--
-- Function qtest_get_java_boolean
--
DROP FUNCTION IF EXISTS qtest_get_java_boolean;
CREATE FUNCTION qtest_get_java_boolean AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaBoolean';

reset;
set hive.stats.dbclass=fs;
