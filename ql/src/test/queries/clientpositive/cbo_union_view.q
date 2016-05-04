set hive.mapred.mode=nonstrict;
set hive.optimize.constant.propagation=false;

CREATE TABLE src_union_1 (key int, value string) PARTITIONED BY (ds string);
CREATE TABLE src_union_2 (key int, value string) PARTITIONED BY (ds string, part_1 string);
CREATE TABLE src_union_3 (key int, value string) PARTITIONED BY (ds string, part_1 string, part_2 string);

CREATE VIEW src_union_view PARTITIONED ON (ds) as
SELECT key, value, ds FROM (
SELECT key, value, ds FROM src_union_1
UNION ALL
SELECT key, value, ds FROM src_union_2
UNION ALL
SELECT key, value, ds FROM src_union_3
) subq;

EXPLAIN SELECT key, value, ds FROM src_union_view WHERE key=86;

EXPLAIN SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='1';
