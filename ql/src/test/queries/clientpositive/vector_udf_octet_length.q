--! qt:dataset:src1
set hive.stats.column.autogather=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DESCRIBE FUNCTION octet_length;
DESCRIBE FUNCTION EXTENDED octet_length;

CREATE TABLE dest1_n51(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1_n51 SELECT octet_length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1_n51 SELECT octet_length(src1.value);
-- SORT_BEFORE_DIFF
SELECT dest1_n51.* FROM dest1_n51;
DROP TABLE dest1_n51;

-- Test with non-ascii characters.
CREATE TABLE dest1_n51(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1_n51;
INSERT INTO dest1_n51 VALUES(NULL);
CREATE TABLE dest2_n10 STORED AS ORC AS SELECT * FROM dest1_n51;
EXPLAIN SELECT octet_length(dest2_n10.name) FROM dest2_n10;
-- SORT_BEFORE_DIFF
SELECT octet_length(dest2_n10.name) FROM dest2_n10;
DROP TABLE dest1_n51;
DROP TABLE dest2_n10;
