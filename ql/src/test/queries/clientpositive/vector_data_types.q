set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

DROP TABLE over1k_n8;
DROP TABLE over1korc_n1;

-- data setup
CREATE TABLE over1k_n8(t tinyint,
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

-- TODO: Remove this line after fixing HIVE-24351
set hive.vectorized.execution.enabled=false;

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE over1k_n8;

CREATE TABLE over1korc_n1(t tinyint,
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

INSERT INTO TABLE over1korc_n1 SELECT * FROM over1k_n8;

-- Add a single NULL row that will come from ORC as isRepeated.
insert into over1korc_n1 values (NULL, NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL, NULL,NULL);

SET hive.vectorized.execution.enabled=false;

EXPLAIN SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc_n1 ORDER BY t, si, i LIMIT 20;

SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc_n1 ORDER BY t, si, i LIMIT 20;

SELECT SUM(HASH(*))
FROM (SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc_n1 ORDER BY t, si, i) as q;

SET hive.vectorized.execution.enabled=true;

EXPLAIN VECTORIZATION EXPRESSION select t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc_n1 ORDER BY t, si, i LIMIT 20;

SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc_n1 ORDER BY t, si, i LIMIT 20;

EXPLAIN VECTORIZATION EXPRESSION 
SELECT SUM(HASH(*))
FROM (SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc_n1 ORDER BY t, si, i) as q;

SELECT SUM(HASH(*))
FROM (SELECT t, si, i, b, f, d, bo, s, ts, `dec`, bin FROM over1korc_n1 ORDER BY t, si, i) as q;
