--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.orcfile.stripe.level=true;
set hive.exec.dynamic.partition=true;

DROP TABLE orcfile_merge2a_n0;

CREATE TABLE orcfile_merge2a_n0 (key INT, value STRING)
    PARTITIONED BY (one string, two string, three string)
    STORED AS ORC;

EXPLAIN INSERT OVERWRITE TABLE orcfile_merge2a_n0 PARTITION (one='1', two, three)
    SELECT key, value, PMOD(HASH(key), 10) as two, 
        PMOD(HASH(value), 10) as three
    FROM src;

INSERT OVERWRITE TABLE orcfile_merge2a_n0 PARTITION (one='1', two, three)
    SELECT key, value, PMOD(HASH(key), 10) as two, 
        PMOD(HASH(value), 10) as three
    FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge2a_n0/one=1/two=0/three=2/;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge2a_n0
) t;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(key, value, '1', PMOD(HASH(key), 10), 
        PMOD(HASH(value), 10)) USING 'tr \t _' AS (c)
    FROM src
) t;

DROP TABLE orcfile_merge2a_n0;

