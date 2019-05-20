--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.orcfile.stripe.level=true;
set hive.merge.sparkfiles=true;

DROP TABLE orcfile_merge3a_n0;
DROP TABLE orcfile_merge3b_n0;

CREATE TABLE orcfile_merge3a_n0 (key int, value string) 
    PARTITIONED BY (ds string) STORED AS TEXTFILE;
CREATE TABLE orcfile_merge3b_n0 (key int, value string) STORED AS ORC;

INSERT OVERWRITE TABLE orcfile_merge3a_n0 PARTITION (ds='1')
    SELECT * FROM src;
INSERT OVERWRITE TABLE orcfile_merge3a_n0 PARTITION (ds='2')
    SELECT * FROM src;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
EXPLAIN INSERT OVERWRITE TABLE orcfile_merge3b_n0
    SELECT key, value FROM orcfile_merge3a_n0;

INSERT OVERWRITE TABLE orcfile_merge3b_n0
    SELECT key, value FROM orcfile_merge3a_n0;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge3b_n0/;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM orcfile_merge3a_n0
) t;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value) USING 'tr \t _' AS (c)
    FROM orcfile_merge3b_n0
) t;

DROP TABLE orcfile_merge3a_n0;
DROP TABLE orcfile_merge3b_n0;
