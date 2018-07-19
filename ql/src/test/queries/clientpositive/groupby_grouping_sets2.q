set hive.explain.user=false;
SET hive.vectorized.execution.enabled=false;
set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.new.job.grouping.set.cardinality=2;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_n81(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_n81;

-- Since 4 grouping sets would be generated for the query below, an additional MR job should be created
EXPLAIN
SELECT a, b, count(*) from T1_n81 group by a, b with cube;

EXPLAIN
SELECT a, b, count(*) from T1_n81 group by cube(a, b);
SELECT a, b, count(*) from T1_n81 group by a, b with cube;

EXPLAIN
SELECT a, b, sum(c) from T1_n81 group by a, b with cube;
SELECT a, b, sum(c) from T1_n81 group by a, b with cube;

CREATE TABLE T2_n50(a STRING, b STRING, c int, d int);

INSERT OVERWRITE TABLE T2_n50
SELECT a, b, c, c from T1_n81;

EXPLAIN
SELECT a, b, sum(c+d) from T2_n50 group by a, b with cube;
SELECT a, b, sum(c+d) from T2_n50 group by a, b with cube;
