--! qt:dataset:src1
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION character_length;
DESCRIBE FUNCTION EXTENDED character_length;

DESCRIBE FUNCTION char_length;
DESCRIBE FUNCTION EXTENDED char_length;

CREATE TABLE dest1_n97(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1_n97 SELECT character_length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1_n97 SELECT character_length(src1.value);
-- SORT_BEFORE_DIFF
SELECT dest1_n97.* FROM dest1_n97;
DROP TABLE dest1_n97;

-- Test with non-ascii characters.
CREATE TABLE dest1_n97(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1_n97;
INSERT INTO dest1_n97 VALUES(NULL);

EXPLAIN SELECT character_length(dest1_n97.name) FROM dest1_n97;
-- SORT_BEFORE_DIFF
SELECT character_length(dest1_n97.name) FROM dest1_n97;

EXPLAIN SELECT char_length(dest1_n97.name) FROM dest1_n97;
-- SORT_BEFORE_DIFF
SELECT char_length(dest1_n97.name) FROM dest1_n97;
DROP TABLE dest1_n97;
