SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION octet_length;
DESCRIBE FUNCTION EXTENDED octet_length;

CREATE TABLE dest1(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1 SELECT octet_length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1 SELECT octet_length(src1.value);
-- SORT_BEFORE_DIFF
SELECT dest1.* FROM dest1;
DROP TABLE dest1;

-- Test with non-ascii characters.
CREATE TABLE dest1(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1;
INSERT INTO dest1 VALUES(NULL);
EXPLAIN SELECT octet_length(dest1.name) FROM dest1;
-- SORT_BEFORE_DIFF
SELECT octet_length(dest1.name) FROM dest1;
DROP TABLE dest1;
