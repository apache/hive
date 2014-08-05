set hive.merge.orcfile.stripe.level=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE orcfile_merge1;
DROP TABLE orcfile_merge1b;

CREATE TABLE orcfile_merge1 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge1b (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;

-- Use non stipe-level merge
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1 PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 100) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 100) as part
    FROM src;

DESC FORMATTED orcfile_merge1 partition (ds='1', part='50');

set hive.merge.orcfile.stripe.level=true;
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1b PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 100) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1b PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 100) as part
    FROM src;

DESC FORMATTED orcfile_merge1 partition (ds='1', part='50');

-- Verify
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1 WHERE ds='1'
) t;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1b WHERE ds='1'
) t;

DROP TABLE orcfile_merge1;
DROP TABLE orcfile_merge1b;
