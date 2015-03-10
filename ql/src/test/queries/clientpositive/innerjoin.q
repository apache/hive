set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 INNER JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

FROM src src1 INNER JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1 SELECT src1.key, src2.value;

SELECT dest_j1.* FROM dest_j1;

-- verify that INNER is a non-reserved word for backwards compatibility
-- change from HIVE-6617, inner is a SQL2011 reserved keyword.
create table `inner`(i int);

select i from `inner`;

create table i(`inner` int);

select `inner` from i;

explain select * from (select * from src) `inner` left outer join src
on `inner`.key=src.key;
