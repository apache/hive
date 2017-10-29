set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE over1k(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(20,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k;

CREATE TABLE t1(`dec` decimal(22,2)) STORED AS ORC;
INSERT INTO TABLE t1 select `dec` from over1k;
CREATE TABLE t2(`dec` decimal(24,0)) STORED AS ORC;
INSERT INTO TABLE t2 select `dec` from over1k;

explain vectorization detail
select t1.`dec`, t2.`dec` from t1 join t2 on (t1.`dec`=t2.`dec`);

-- SORT_QUERY_RESULTS

select t1.`dec`, t2.`dec` from t1 join t2 on (t1.`dec`=t2.`dec`);

-- DECIMAL_64

CREATE TABLE over1k_small(t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k_small;

CREATE TABLE t1_small(`dec` decimal(4,2)) STORED AS ORC;
INSERT INTO TABLE t1 select `dec` from over1k_small;
CREATE TABLE t2_small(`dec` decimal(4,0)) STORED AS ORC;
INSERT INTO TABLE t2 select `dec` from over1k_small;

explain vectorization detail
select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);

-- SORT_QUERY_RESULTS

select t1_small.`dec`, t2_small.`dec` from t1_small join t2_small on (t1_small.`dec`=t2_small.`dec`);
