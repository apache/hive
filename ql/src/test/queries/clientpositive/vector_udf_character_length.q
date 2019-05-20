--! qt:dataset:src1
set hive.stats.column.autogather=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DESCRIBE FUNCTION character_length;
DESCRIBE FUNCTION EXTENDED character_length;

DESCRIBE FUNCTION char_length;
DESCRIBE FUNCTION EXTENDED char_length;

CREATE TABLE dest1_n59(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1_n59 SELECT character_length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1_n59 SELECT character_length(src1.value);
-- SORT_BEFORE_DIFF
SELECT dest1_n59.* FROM dest1_n59;
DROP TABLE dest1_n59;

-- Test with non-ascii characters.
CREATE TABLE dest1_n59(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1_n59;
INSERT INTO dest1_n59 VALUES(NULL);
CREATE TABLE dest2_n13 STORED AS ORC AS SELECT * FROM dest1_n59;

EXPLAIN SELECT character_length(dest2_n13.name) FROM dest2_n13;
-- SORT_BEFORE_DIFF
SELECT character_length(dest2_n13.name) FROM dest2_n13;

EXPLAIN SELECT char_length(dest2_n13.name) FROM dest2_n13;
-- SORT_BEFORE_DIFF
SELECT char_length(dest2_n13.name) FROM dest2_n13;
DROP TABLE dest1_n59;
DROP TABLE dest2_n13;
