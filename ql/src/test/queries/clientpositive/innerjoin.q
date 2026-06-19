--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n20(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 INNER JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n20 SELECT src1.key, src2.value;

FROM src src1 INNER JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n20 SELECT src1.key, src2.value;

SELECT dest_j1_n20.* FROM dest_j1_n20;

-- verify that INNER is a non-reserved word for backwards compatibility
-- change from HIVE-6617, inner is a SQL2011 reserved keyword.
create table `inner`(i_n2 int);

select i_n2 from `inner`;

create table i_n2(`inner` int);

select `inner` from i_n2;

explain select * from (select * from src) `inner` left outer join src
on `inner`.key=src.key;
