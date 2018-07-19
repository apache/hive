--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.stats.dbclass=fs;
set hive.explain.user=false;

CREATE TABLE src_union_1_n0 (key int, value string) PARTITIONED BY (ds string);

CREATE TABLE src_union_2_n0 (key int, value string) PARTITIONED BY (ds string, part_1 string);

CREATE TABLE src_union_3_n0(key int, value string) PARTITIONED BY (ds string, part_1 string, part_2 string);

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SET hive.optimize.index.filter=true;

SET hive.exec.pre.hooks=;
SET hive.exec.post.hooks=;
SET hive.semantic.analyzer.hook=;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;

INSERT OVERWRITE TABLE src_union_1_n0 PARTITION (ds='1') SELECT * FROM src;

INSERT OVERWRITE TABLE src_union_2_n0 PARTITION (ds='2', part_1='1') SELECT * FROM src;
INSERT OVERWRITE TABLE src_union_2_n0 PARTITION (ds='2', part_1='2') SELECT * FROM src;

INSERT OVERWRITE TABLE src_union_3_n0 PARTITION (ds='3', part_1='1', part_2='2:3+4') SELECT * FROM src;
INSERT OVERWRITE TABLE src_union_3_n0 PARTITION (ds='3', part_1='2', part_2='2:3+4') SELECT * FROM src;

EXPLAIN SELECT key, value, ds FROM src_union_1_n0 WHERE key=86 and ds='1';
EXPLAIN SELECT key, value, ds FROM src_union_2_n0 WHERE key=86 and ds='2';
EXPLAIN SELECT key, value, ds FROM src_union_3_n0 WHERE key=86 and ds='3';

SELECT key, value, ds FROM src_union_1_n0 WHERE key=86 AND ds ='1';
SELECT key, value, ds FROM src_union_2_n0 WHERE key=86 AND ds ='2';
SELECT key, value, ds FROM src_union_3_n0 WHERE key=86 AND ds ='3';

EXPLAIN SELECT count(1) from src_union_1_n0 WHERE ds ='1';
EXPLAIN SELECT count(1) from src_union_2_n0 WHERE ds ='2';
EXPLAIN SELECT count(1) from src_union_3_n0 WHERE ds ='3';

SELECT count(1) from src_union_1_n0 WHERE ds ='1';
SELECT count(1) from src_union_2_n0 WHERE ds ='2';
SELECT count(1) from src_union_3_n0 WHERE ds ='3';

CREATE VIEW src_union_view_n0 PARTITIONED ON (ds) as
SELECT key, value, ds FROM (
SELECT key, value, ds FROM src_union_1_n0
UNION ALL
SELECT key, value, ds FROM src_union_2_n0
UNION ALL
SELECT key, value, ds FROM src_union_3_n0
) subq;

EXPLAIN SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='1';
EXPLAIN SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='2';
EXPLAIN SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='3';
EXPLAIN SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds IS NOT NULL;

SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='1';
SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='2';
SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='3';
-- SORT_BEFORE_DIFF
SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds IS NOT NULL;

EXPLAIN SELECT count(1) from src_union_view_n0 WHERE ds ='1';
EXPLAIN SELECT count(1) from src_union_view_n0 WHERE ds ='2';
EXPLAIN SELECT count(1) from src_union_view_n0 WHERE ds ='3';

SELECT count(1) from src_union_view_n0 WHERE ds ='1';
SELECT count(1) from src_union_view_n0 WHERE ds ='2';
SELECT count(1) from src_union_view_n0 WHERE ds ='3';

INSERT OVERWRITE TABLE src_union_3_n0 PARTITION (ds='4', part_1='1', part_2='2:3+4') SELECT * FROM src;

EXPLAIN SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='4';
SELECT key, value, ds FROM src_union_view_n0 WHERE key=86 AND ds ='4';

EXPLAIN SELECT count(1) from src_union_view_n0 WHERE ds ='4';
SELECT count(1) from src_union_view_n0 WHERE ds ='4';
