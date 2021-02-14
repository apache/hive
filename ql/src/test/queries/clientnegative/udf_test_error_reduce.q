--! qt:dataset:src
set hive.llap.execution.mode=auto;
CREATE TEMPORARY FUNCTION test_error AS 'org.apache.hadoop.hive.ql.udf.UDFTestErrorOnFalse';

SELECT test_error(key < 125 OR key > 130)
FROM (
  SELECT *
  FROM src
  DISTRIBUTE BY rand()
) map_output;


