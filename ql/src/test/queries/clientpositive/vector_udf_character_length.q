SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DESCRIBE FUNCTION character_length;
DESCRIBE FUNCTION EXTENDED character_length;

DESCRIBE FUNCTION char_length;
DESCRIBE FUNCTION EXTENDED char_length;

CREATE TABLE dest1(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1 SELECT character_length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1 SELECT character_length(src1.value);
-- SORT_BEFORE_DIFF
SELECT dest1.* FROM dest1;
DROP TABLE dest1;

-- Test with non-ascii characters.
CREATE TABLE dest1(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1;
INSERT INTO dest1 VALUES(NULL);
CREATE TABLE dest2 STORED AS ORC AS SELECT * FROM dest1;

EXPLAIN SELECT character_length(dest2.name) FROM dest2;
-- SORT_BEFORE_DIFF
SELECT character_length(dest2.name) FROM dest2;

EXPLAIN SELECT char_length(dest2.name) FROM dest2;
-- SORT_BEFORE_DIFF
SELECT char_length(dest2.name) FROM dest2;
DROP TABLE dest1;
DROP TABLE dest2;
