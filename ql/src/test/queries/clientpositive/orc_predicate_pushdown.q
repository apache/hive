set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

-- SORT_QUERY_RESULTS

CREATE TABLE orc_pred(t tinyint,
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

ALTER TABLE orc_pred SET SERDEPROPERTIES ('orc.row.index.stride' = '1000');

CREATE TABLE staging_n2(t tinyint,
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

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging_n2;

INSERT INTO TABLE orc_pred select * from staging_n2;

-- no predicate case. the explain plan should not have filter expression in table scan operator

SELECT SUM(HASH(t)) FROM orc_pred;

SELECT SUM(HASH(t)) FROM orc_pred;

EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred;

EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred;

-- all the following queries have predicates which are pushed down to table scan operator if
-- predicate push-down is enabled. the explain plan should show filter expression
-- in table scan operator.

SELECT * FROM orc_pred WHERE t<2 limit 1;
SELECT * FROM orc_pred WHERE t<2 limit 1;

SELECT * FROM orc_pred WHERE t>2 limit 1;
SELECT * FROM orc_pred WHERE t>2 limit 1;

SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;

SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;

EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;

EXPLAIN SELECT SUM(HASH(t)) FROM orc_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2;

SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;

SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;

EXPLAIN SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;

EXPLAIN SELECT t, s FROM orc_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;

SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

EXPLAIN SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

EXPLAIN SELECT t, s FROM orc_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;

SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;

EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;

EXPLAIN SELECT t, si, d, s FROM orc_pred
  WHERE t > 10
  AND t <> 101
  AND d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  SORT BY s DESC
  LIMIT 3;
