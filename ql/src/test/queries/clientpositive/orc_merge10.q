--! qt:dataset:src
--! qt:dataset:part
--! qt:replace:/(File Version:)(.+)/$1#Masked#/
set hive.vectorized.execution.enabled=false;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.orcfile.stripe.level=false;
set hive.exec.dynamic.partition=true;
set mapred.min.split.size=1000;
set mapred.max.split.size=2000;
set tez.am.grouping.split-count=2;
set tez.grouping.split-count=2;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

-- SORT_QUERY_RESULTS

DROP TABLE orcfile_merge1;
DROP TABLE orcfile_merge1b;
DROP TABLE orcfile_merge1c;

CREATE TABLE orcfile_merge1 (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC tblproperties("orc.compress"="SNAPPY","orc.compress.size"="4096");
CREATE TABLE orcfile_merge1b (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC tblproperties("orc.compress"="SNAPPY","orc.compress.size"="4096");
CREATE TABLE orcfile_merge1c (key INT, value STRING) 
    PARTITIONED BY (ds STRING, part STRING) STORED AS ORC tblproperties("orc.compress"="SNAPPY","orc.compress.size"="4096");

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

select count(*) from orcfile_merge1;
select count(*) from orcfile_merge1b;

set tez.am.grouping.split-count=1;
set tez.grouping.split-count=1;
-- concatenate
explain ALTER TABLE  orcfile_merge1 PARTITION (ds='1', part='0') CONCATENATE;
ALTER TABLE  orcfile_merge1 PARTITION (ds='1', part='0') CONCATENATE;

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orcfile_merge1/ds=1/part=0/;

-- Verify
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1c WHERE ds='1'
) t;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c)
    FROM orcfile_merge1 WHERE ds='1'
) t;

select count(*) from orcfile_merge1;
select count(*) from orcfile_merge1c;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecOrcFileDump;
select * from orcfile_merge1 where ds='1' and part='0' limit 1;
select * from orcfile_merge1c where ds='1' and part='0' limit 1;

DROP TABLE orcfile_merge1;
DROP TABLE orcfile_merge1b;
DROP TABLE orcfile_merge1c;
