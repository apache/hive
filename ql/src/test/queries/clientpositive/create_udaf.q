--! qt:dataset:src
EXPLAIN
CREATE TEMPORARY FUNCTION test_max AS 'org.apache.hadoop.hive.ql.udf.UDAFTestMax';

CREATE TEMPORARY FUNCTION test_max AS 'org.apache.hadoop.hive.ql.udf.UDAFTestMax';

CREATE TABLE dest1_n34(col INT);

FROM src INSERT OVERWRITE TABLE dest1_n34 SELECT test_max(length(src.value));

SELECT dest1_n34.* FROM dest1_n34;

-- cover all the other value types:
SELECT test_max(CAST(length(src.value) AS SMALLINT)) FROM src;
SELECT test_max(CAST(length(src.value) AS BIGINT)) FROM src;
SELECT test_max(CAST(length(src.value) AS DOUBLE)) FROM src;
SELECT test_max(CAST(length(src.value) AS FLOAT)) FROM src;
SELECT test_max(substr(src.value,5)) FROM src;

DROP TEMPORARY FUNCTION test_max;
