--! qt:dataset:src1
DESCRIBE FUNCTION reverse;
DESCRIBE FUNCTION EXTENDED reverse;

CREATE TABLE dest1_n44(len STRING);
EXPLAIN FROM src1 INSERT OVERWRITE TABLE dest1_n44 SELECT reverse(src1.value);
FROM src1 INSERT OVERWRITE TABLE dest1_n44 SELECT reverse(src1.value);
SELECT dest1_n44.* FROM dest1_n44;
DROP TABLE dest1_n44;

-- Test with non-ascii characters
-- kv4.txt contains the text 0xE982B5E993AE, which should be reversed to
-- 0xE993AEE982B5
CREATE TABLE dest1_n44(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE dest1_n44;
SELECT count(1) FROM dest1_n44 WHERE reverse(dest1_n44.name) = _UTF-8 0xE993AEE982B5;
