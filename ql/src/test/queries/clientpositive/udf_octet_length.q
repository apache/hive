--! qt:dataset:src1
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION octet_length;
DESCRIBE FUNCTION EXTENDED octet_length;

CREATE TABLE dest1_n165(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1_n165 SELECT octet_length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1_n165 SELECT octet_length(src1.value);
-- SORT_BEFORE_DIFF
SELECT dest1_n165.* FROM dest1_n165;
DROP TABLE dest1_n165;

-- Test with non-ascii characters.
CREATE TABLE dest1_n165(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1_n165;
INSERT INTO dest1_n165 VALUES(NULL);
EXPLAIN SELECT octet_length(dest1_n165.name) FROM dest1_n165;
-- SORT_BEFORE_DIFF
SELECT octet_length(dest1_n165.name) FROM dest1_n165;
DROP TABLE dest1_n165;
