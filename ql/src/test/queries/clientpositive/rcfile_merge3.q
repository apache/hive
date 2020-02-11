--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.merge.rcfile.block.level=true;
set mapred.max.split.size=100;
set mapred.min.split.size=1;

DROP TABLE rcfile_merge3a_n0;
DROP TABLE rcfile_merge3b_n0;

CREATE TABLE rcfile_merge3a_n0 (key int, value string) 
    PARTITIONED BY (ds string) STORED AS TEXTFILE;
CREATE TABLE rcfile_merge3b_n0 (key int, value string) STORED AS RCFILE;

INSERT OVERWRITE TABLE rcfile_merge3a_n0 PARTITION (ds='1')
    SELECT * FROM src;
INSERT OVERWRITE TABLE rcfile_merge3a_n0 PARTITION (ds='2')
    SELECT * FROM src;

EXPLAIN INSERT OVERWRITE TABLE rcfile_merge3b_n0
    SELECT key, value FROM rcfile_merge3a_n0;
INSERT OVERWRITE TABLE rcfile_merge3b_n0
    SELECT key, value FROM rcfile_merge3a_n0;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM rcfile_merge3a_n0
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM rcfile_merge3b_n0
) t;

DROP TABLE rcfile_merge3a_n0;
DROP TABLE rcfile_merge3b_n0;
