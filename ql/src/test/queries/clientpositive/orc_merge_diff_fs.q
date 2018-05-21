--! qt:dataset:src
--! qt:dataset:part

set hive.vectorized.execution.enabled=false;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.orcfile.stripe.level=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set mapred.min.split.size=1000;
set mapred.max.split.size=2000;
set tez.grouping.min-size=1000;
set tez.grouping.max-size=2000;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.merge.sparkfiles=false;

set hive.metastore.warehouse.dir=pfile://${system:test.tmp.dir}/orc_merge_diff_fs;

-- SORT_QUERY_RESULTS

DROP TABLE orcfile_merge1_n0;
DROP TABLE orcfile_merge1b_n0;
DROP TABLE orcfile_merge1c_n0;

CREATE TABLE orcfile_merge1_n0 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge1b_n0 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge1c_n0 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;

-- merge disabled
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1_n0 PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1_n0 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 2) as part
    FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1_n0/ds=1/part=0/;

set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.sparkfiles=true;
-- auto-merge slow way
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1b_n0 PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1b_n0 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 2) as part
    FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1b_n0/ds=1/part=0/;

set hive.merge.orcfile.stripe.level=true;
-- auto-merge fast way
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1c_n0 PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1c_n0 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 2) as part
    FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1c_n0/ds=1/part=0/;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
-- Verify
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1_n0 WHERE ds='1'
) t;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1b_n0 WHERE ds='1'
) t;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1c_n0 WHERE ds='1'
) t;

select count(*) from orcfile_merge1_n0;
select count(*) from orcfile_merge1b_n0;
select count(*) from orcfile_merge1c_n0;

DROP TABLE orcfile_merge1_n0;
DROP TABLE orcfile_merge1b_n0;
DROP TABLE orcfile_merge1c_n0;
