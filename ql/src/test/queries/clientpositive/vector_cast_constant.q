set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
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
           dec decimal(4,2),
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
           dec decimal(4,2),
           bin binary)
STORED AS ORC;

INSERT INTO TABLE over1korc SELECT * FROM over1k;

EXPLAIN VECTORIZATION EXPRESSION SELECT 
  i,
  AVG(CAST(50 AS INT)) AS `avg_int_ok`,
  AVG(CAST(50 AS DOUBLE)) AS `avg_double_ok`,
  AVG(CAST(50 AS DECIMAL)) AS `avg_decimal_ok`
  FROM over1korc GROUP BY i ORDER BY i LIMIT 10;

SELECT 
  i,
  AVG(CAST(50 AS INT)) AS `avg_int_ok`,
  AVG(CAST(50 AS DOUBLE)) AS `avg_double_ok`,
  AVG(CAST(50 AS DECIMAL)) AS `avg_decimal_ok`
  FROM over1korc GROUP BY i ORDER BY i LIMIT 10;
