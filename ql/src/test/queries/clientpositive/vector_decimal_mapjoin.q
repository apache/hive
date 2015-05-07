SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.vectorized.execution.enabled=true;

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

CREATE TABLE t1(dec decimal(4,2)) STORED AS ORC;
INSERT INTO TABLE t1 select dec from over1k;
CREATE TABLE t2(dec decimal(4,0)) STORED AS ORC;
INSERT INTO TABLE t2 select dec from over1k;

explain
select t1.dec, t2.dec from t1 join t2 on (t1.dec=t2.dec);

-- SORT_QUERY_RESULTS

select t1.dec, t2.dec from t1 join t2 on (t1.dec=t2.dec);
