set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

DROP TABLE over1k;
DROP TABLE over1korc;

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

CREATE TABLE over1korc(t tinyint,
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

INSERT INTO TABLE over1korc SELECT * FROM over1k;

SET hive.vectorized.execution.enabled=false;

EXPLAIN VECTORIZATION EXPRESSION SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc ORDER BY t, si, i LIMIT 20;

SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc ORDER BY t, si, i LIMIT 20;

SELECT SUM(HASH(*))
FROM (SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc ORDER BY t, si, i) as q;

SET hive.vectorized.execution.enabled=true;

EXPLAIN VECTORIZATION EXPRESSION select t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc ORDER BY t, si, i LIMIT 20;

SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc ORDER BY t, si, i LIMIT 20;

SELECT SUM(HASH(*))
FROM (SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc ORDER BY t, si, i) as q;
