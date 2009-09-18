EXPLAIN
CREATE TEMPORARY FUNCTION test_max AS 'org.apache.hadoop.hive.ql.udf.UDAFTestMax';

CREATE TEMPORARY FUNCTION test_max AS 'org.apache.hadoop.hive.ql.udf.UDAFTestMax';

CREATE TABLE dest1(col INT);

FROM src INSERT OVERWRITE TABLE dest1 SELECT test_max(length(src.value));

SELECT dest1.* FROM dest1;

DROP TEMPORARY FUNCTION test_max;
