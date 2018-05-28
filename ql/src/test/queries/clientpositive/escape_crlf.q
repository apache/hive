set hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=disable;

DROP TABLE IF EXISTS base_tab_n0;
CREATE TABLE base_tab_n0(a STRING, b STRING)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|';
DESCRIBE EXTENDED base_tab_n0;

LOAD DATA LOCAL INPATH '../../data/files/escape_crlf.txt' OVERWRITE INTO TABLE base_tab_n0;
-- No crlf escaping
SELECT * FROM base_tab_n0;

-- Crlf escaping
ALTER TABLE base_tab_n0 SET SERDEPROPERTIES ('escape.delim'='\\', 'serialization.escape.crlf'='true');
SELECT * FROM base_tab_n0;

SET hive.fetch.task.conversion=none;
-- Make sure intermediate serde works correctly
SELECT * FROM base_tab_n0;

DROP TABLE base_tab_n0;
