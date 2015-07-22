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

-- SORT_QUERY_RESULTS

DROP TABLE orcfile_merge1;
DROP TABLE orcfile_merge1b;
DROP TABLE orcfile_merge1c;

CREATE TABLE orcfile_merge1 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge1b (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;
CREATE TABLE orcfile_merge1c (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC;

-- merge disabled
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1 PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1 PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 2) as part
    FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1/ds=1/part=0/;

set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
-- auto-merge slow way
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1b PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1b PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 2) as part
    FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1b/ds=1/part=0/;

set hive.merge.orcfile.stripe.level=true;
-- auto-merge fast way
EXPLAIN
    INSERT OVERWRITE TABLE orcfile_merge1c PARTITION (ds='1', part)
        SELECT key, value, PMOD(HASH(key), 2) as part
        FROM src;

INSERT OVERWRITE TABLE orcfile_merge1c PARTITION (ds='1', part)
    SELECT key, value, PMOD(HASH(key), 2) as part
    FROM src;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1c/ds=1/part=0/;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
-- Verify
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1 WHERE ds='1'
) t;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1b WHERE ds='1'
) t;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1c WHERE ds='1'
) t;

select count(*) from orcfile_merge1;
select count(*) from orcfile_merge1b;
select count(*) from orcfile_merge1c;

DROP TABLE orcfile_merge1;
DROP TABLE orcfile_merge1b;
DROP TABLE orcfile_merge1c;
