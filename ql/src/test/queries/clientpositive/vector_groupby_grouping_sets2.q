set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.new.job.grouping.set.cardinality=2;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_text(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_text;

CREATE TABLE T1 STORED AS ORC AS SELECT * FROM T1_text;

-- Since 4 grouping sets would be generated for the query below, an additional MR job should be created
EXPLAIN
SELECT a, b, count(*) from T1 group by a, b with cube;

EXPLAIN
SELECT a, b, count(*) from T1 group by cube(a, b);
SELECT a, b, count(*) from T1 group by a, b with cube;

EXPLAIN
SELECT a, b, sum(c) from T1 group by a, b with cube;
SELECT a, b, sum(c) from T1 group by a, b with cube;

CREATE TABLE T2(a STRING, b STRING, c int, d int) STORED AS ORC;

INSERT OVERWRITE TABLE T2
SELECT a, b, c, c from T1;

EXPLAIN
SELECT a, b, sum(c+d) from T2 group by a, b with cube;
SELECT a, b, sum(c+d) from T2 group by a, b with cube;
