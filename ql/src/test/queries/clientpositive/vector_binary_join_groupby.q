SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;

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
           dec decimal(4,2),
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
           dec decimal(4,2),
           bin binary)
STORED AS ORC;

INSERT INTO TABLE hundredorc SELECT * FROM over1k LIMIT 100;

EXPLAIN 
SELECT sum(hash(*))
FROM hundredorc t1 JOIN hundredorc t2 ON t1.bin = t2.bin;

SELECT sum(hash(*))
FROM hundredorc t1 JOIN hundredorc t2 ON t2.bin = t2.bin;

EXPLAIN 
SELECT count(*), bin
FROM hundredorc
GROUP BY bin;

SELECT count(*), bin
FROM hundredorc
GROUP BY bin;
