--! qt:dataset:src1
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION length;
DESCRIBE FUNCTION EXTENDED length;

CREATE TABLE dest1_n134(len INT);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1_n134 SELECT length(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1_n134 SELECT length(src1.value);
SELECT dest1_n134.* FROM dest1_n134;
DROP TABLE dest1_n134;

-- Test with non-ascii characters. 
CREATE TABLE dest1_n134(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1_n134;
EXPLAIN SELECT length(dest1_n134.name) FROM dest1_n134;
SELECT length(dest1_n134.name) FROM dest1_n134;
