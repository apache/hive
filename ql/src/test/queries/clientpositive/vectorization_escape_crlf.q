set hive.vectorized.execution.enabled=true;
set hive.test.vectorized.execution.enabled.override=enable;

DROP TABLE IF EXISTS base_tab;
CREATE TABLE base_tab(a STRING, b STRING)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|';
DESCRIBE EXTENDED base_tab;

LOAD DATA LOCAL INPATH '../../data/files/escape_crlf.txt' OVERWRITE INTO TABLE base_tab;
-- No crlf escaping
SELECT * FROM base_tab;

-- Crlf escaping
ALTER TABLE base_tab SET SERDEPROPERTIES ('escape.delim'='\\', 'serialization.escape.crlf'='true');
SELECT * FROM base_tab;

SET hive.fetch.task.conversion=none;
-- Make sure intermediate serde works correctly
SELECT * FROM base_tab;

DROP TABLE base_tab;
