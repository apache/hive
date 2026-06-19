set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
SET hive.optimize.ppd=true;
set hive.llap.cache.allow.synthetic.fileid=true;

-- SORT_QUERY_RESULTS
CREATE TABLE tbl_pred(t tinyint,
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
STORED AS PARQUET;

CREATE TABLE staging_n0(t tinyint,
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

LOAD DATA LOCAL INPATH '../../data/files/over1k' OVERWRITE INTO TABLE staging_n0;

INSERT INTO TABLE tbl_pred select * from staging_n0;

-- no predicate case. the explain plan should not have filter expression in table scan operator

SELECT SUM(HASH(t)) FROM tbl_pred;

SET hive.optimize.index.filter=true;
SELECT SUM(HASH(t)) FROM tbl_pred;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT SUM(HASH(t)) FROM tbl_pred;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT SUM(HASH(t)) FROM tbl_pred;
SET hive.optimize.index.filter=false;

-- all the following queries have predicates which are pushed down to table scan operator if
-- hive.optimize.index.filter is set to true. the explain plan should show filter expression
-- in table scan operator.

SELECT * FROM tbl_pred WHERE t<2 limit 1;
SET hive.optimize.index.filter=true;
SELECT * FROM tbl_pred WHERE t<2 limit 1;
SET hive.optimize.index.filter=false;

SELECT * FROM tbl_pred WHERE t>2 limit 1;
SET hive.optimize.index.filter=true;
SELECT * FROM tbl_pred WHERE t>2 limit 1;
SET hive.optimize.index.filter=false;

SELECT * FROM tbl_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2
  LIMIT 10;

SET hive.optimize.index.filter=true;
SELECT * FROM tbl_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2
  LIMIT 10;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT * FROM tbl_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2
  LIMIT 10;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT * FROM tbl_pred
  WHERE t IS NOT NULL
  AND t < 0
  AND t > -2
  LIMIT 10;
SET hive.optimize.index.filter=false;

SELECT t, s FROM tbl_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;

SET hive.optimize.index.filter=true;
SELECT t, s FROM tbl_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT t, s FROM tbl_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, s FROM tbl_pred
  WHERE t <=> -1
  AND s IS NOT NULL
  AND s LIKE 'bob%'
  ;
SET hive.optimize.index.filter=false;

SELECT t, s FROM tbl_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

set hive.optimize.index.filter=true;
SELECT t, s FROM tbl_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;
set hive.optimize.index.filter=false;

EXPLAIN SELECT t, s FROM tbl_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, s FROM tbl_pred
  WHERE s IS NOT NULL
  AND s LIKE 'bob%'
  AND t NOT IN (-1,-2,-3)
  AND t BETWEEN 25 AND 30
  SORT BY t,s;
SET hive.optimize.index.filter=false;

SELECT t, si, d, s FROM tbl_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

SET hive.optimize.index.filter=true;
SELECT t, si, d, s FROM tbl_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT t, si, d, s FROM tbl_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, si, d, s FROM tbl_pred
  WHERE d >= ROUND(9.99)
  AND d < 12
  AND t IS NOT NULL
  AND s LIKE '%son'
  AND s NOT LIKE '%car%'
  AND t > 0
  AND si BETWEEN 300 AND 400
  ORDER BY s DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;

SELECT t, si, d, s FROM tbl_pred
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

SET hive.optimize.index.filter=true;
SELECT t, si, d, s FROM tbl_pred
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
SET hive.optimize.index.filter=false;

SET hive.optimize.index.filter=true;
SELECT f, i, b FROM tbl_pred
  WHERE f IS NOT NULL
  AND f < 123.2
  AND f > 1.92
  AND f >= 9.99
  AND f BETWEEN 1.92 AND 123.2
  AND i IS NOT NULL
  AND i < 67627
  AND i > 60627
  AND i >= 60626
  AND i BETWEEN 60626 AND 67627
  AND b IS NOT NULL
  AND b < 4294967861
  AND b > 4294967261
  AND b >= 4294967260
  AND b BETWEEN 4294967261 AND 4294967861
  SORT BY f DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;

EXPLAIN SELECT t, si, d, s FROM tbl_pred
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

SET hive.optimize.index.filter=true;
EXPLAIN SELECT t, si, d, s FROM tbl_pred
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
SET hive.optimize.index.filter=false;


SET hive.optimize.index.filter=true;
EXPLAIN SELECT f, i, b FROM tbl_pred
  WHERE f IS NOT NULL
  AND f < 123.2
  AND f > 1.92
  AND f >= 9.99
  AND f BETWEEN 1.92 AND 123.2
  AND i IS NOT NULL
  AND i < 67627
  AND i > 60627
  AND i >= 60626
  AND i BETWEEN 60626 AND 67627
  AND b IS NOT NULL
  AND b < 4294967861
  AND b > 4294967261
  AND b >= 4294967260
  AND b BETWEEN 4294967261 AND 4294967861
  SORT BY f DESC
  LIMIT 3;
SET hive.optimize.index.filter=false;
