set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE over1k;
DROP TABLE hundredorc;

-- data setup
CREATE TABLE over1k(t tinyint,
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

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k;

CREATE TABLE hundredorc(t tinyint,
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
STORED AS ORC;

INSERT INTO TABLE hundredorc SELECT * FROM over1k LIMIT 100;

EXPLAIN VECTORIZATION EXPRESSION
SELECT sum(hash(*)) k
FROM hundredorc t1 JOIN hundredorc t2 ON t1.bin = t2.bin
order by k;

SELECT sum(hash(*)) k
FROM hundredorc t1 JOIN hundredorc t2 ON t1.bin = t2.bin
order by k;

EXPLAIN VECTORIZATION EXPRESSION
SELECT count(*), bin
FROM hundredorc
GROUP BY bin
order by bin;

SELECT count(*), bin
FROM hundredorc
GROUP BY bin
order by bin;

-- HIVE-14045: Involve a binary vector scratch column for small table result (Native Vector MapJoin).

EXPLAIN VECTORIZATION EXPRESSION
SELECT t1.i, t1.bin, t2.bin
FROM hundredorc t1 JOIN hundredorc t2 ON t1.i = t2.i;
