set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.new.job.grouping.set.cardinality=2;

-- SORT_QUERY_RESULTS

CREATE TABLE T1_text_n3(a STRING, b STRING, c STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/grouping_sets.txt' INTO TABLE T1_text_n3;

CREATE TABLE T1_n69 STORED AS ORC AS SELECT * FROM T1_text_n3;

-- Since 4 grouping sets would be generated for the query below, an additional MR job should be created
EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) from T1_n69 group by a, b with cube;

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, count(*) from T1_n69 group by cube(a, b);
SELECT a, b, count(*) from T1_n69 group by a, b with cube;

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, sum(c) from T1_n69 group by a, b with cube;
SELECT a, b, sum(c) from T1_n69 group by a, b with cube;

CREATE TABLE T2_n42(a STRING, b STRING, c int, d int) STORED AS ORC;

INSERT OVERWRITE TABLE T2_n42
SELECT a, b, c, c from T1_n69;

EXPLAIN VECTORIZATION DETAIL
SELECT a, b, sum(c+d) from T2_n42 group by a, b with cube;
SELECT a, b, sum(c+d) from T2_n42 group by a, b with cube;
