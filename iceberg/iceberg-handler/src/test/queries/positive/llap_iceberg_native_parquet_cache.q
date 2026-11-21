--test the native LLAP Parquet cache path against Iceberg tables
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.llap.io.parquet.native.enabled=true;

DROP TABLE IF EXISTS llap_native_orders PURGE;

CREATE EXTERNAL TABLE llap_native_orders (
  orderid INT,
  quantity BIGINT,
  price DECIMAL(7,2),
  total DECIMAL(20,2),
  item STRING,
  orderdate DATE,
  tradets TIMESTAMP,
  shipped BOOLEAN,
  region STRING)
PARTITIONED BY SPEC (region, bucket(4, orderid))
STORED BY ICEBERG STORED AS PARQUET
TBLPROPERTIES ('format-version'='2');

INSERT INTO llap_native_orders VALUES
(0, 48, 35000.10, 1680004.80, 'Model 3', date('2000-06-04'), timestamp('2000-06-04 19:55:46.129'), true, 'EU'),
(1, 12, 45000.25, 540003.00, 'Model 3', date('2007-06-24'), timestamp('2007-06-24 19:23:22.829'), false, 'US'),
(2, 76, 48000.00, 3648000.00, 'Model Y', date('2018-02-19'), timestamp('2018-02-19 23:43:51.995'), true, 'EU'),
(3, 91, 83000.99, 7553090.09, 'Model S', date('2000-07-15'), timestamp('2000-07-15 09:09:11.587'), false, 'US'),
(4, 18, 123000.00, 2214000.00, 'Model S', date('2007-12-02'), timestamp('2007-12-02 22:30:39.302'), true, 'EU');
INSERT INTO llap_native_orders VALUES
(5, 71, 35000.10, 2485007.10, 'Model 3', date('2010-02-08'), timestamp('2010-02-08 20:31:23.430'), NULL, 'EU'),
(6, 78, NULL, NULL, 'Model Y', date('2016-02-22'), timestamp('2016-02-22 20:37:37.025'), false, 'EU'),
(7, 88, 55000.50, 4840044.00, NULL, NULL, NULL, true, 'US'),
(8, 87, 48000.00, 4176000.00, 'Model Y', date('2003-02-20'), timestamp('2003-02-20 00:48:09.139'), true, 'EU'),
(9, 60, 123000.00, 7380000.00, 'Model S', date('2012-08-28'), timestamp('2012-08-28 01:35:54.283'), false, 'US');
INSERT INTO llap_native_orders VALUES
(10, 24, 83000.99, 1992023.76, 'Model S', date('2015-03-28'), timestamp('2015-03-28 18:57:50.069'), true, 'ASIA');

--projections, count(*), partition and data column filters
SELECT * FROM llap_native_orders ORDER BY orderid;
SELECT count(*) FROM llap_native_orders;
--LLAP IO counters are reported only by the native reader, so they show the scan did not fall back
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
SELECT orderid, item, shipped FROM llap_native_orders WHERE region = 'EU' ORDER BY orderid;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter;
SELECT orderid, quantity FROM llap_native_orders WHERE quantity > 70 AND orderdate > date('2005-01-01') ORDER BY orderid;
SELECT orderid, tradets FROM llap_native_orders WHERE tradets < timestamp('2008-01-01 00:00:00') ORDER BY orderid;
SELECT orderid FROM llap_native_orders WHERE item IS NULL OR price IS NULL ORDER BY orderid;

--decimal aggregates
SELECT region, count(*), sum(quantity), min(price), max(price), sum(total), avg(price) FROM llap_native_orders GROUP BY region ORDER BY region;
SELECT item, sum(price * quantity), sum(total) FROM llap_native_orders WHERE shipped GROUP BY item ORDER BY item;

--virtual columns
SELECT orderid, ROW__POSITION, PARTITION__SPEC__ID, PARTITION__HASH, PARTITION__NAME FROM llap_native_orders ORDER BY orderid;
SELECT PARTITION__NAME, count(*), count(distinct FILE__PATH), max(ROW__POSITION) FROM llap_native_orders GROUP BY PARTITION__NAME ORDER BY PARTITION__NAME;

--schema evolution: rename and reorder
ALTER TABLE llap_native_orders CHANGE item product STRING AFTER shipped;
ALTER TABLE llap_native_orders CHANGE price unitprice DECIMAL(7,2) AFTER product;
SELECT orderid, product, unitprice FROM llap_native_orders WHERE unitprice >= 50000 ORDER BY orderid;

--schema evolution: add a column
ALTER TABLE llap_native_orders ADD COLUMNS (discount INT);
INSERT INTO llap_native_orders VALUES
(11, 10, 1000000.00, date('2021-01-04'), timestamp('2021-01-04 19:55:46.129'), true, 'Model X', 93000.00, 'EU', 5),
(12, 11, 1243000.00, date('2021-02-04'), timestamp('2021-02-04 19:55:46.129'), false, 'Model X', 113000.00, 'US', NULL);
SELECT product, min(discount), max(discount), count(*) FROM llap_native_orders GROUP BY product ORDER BY product;

--schema evolution: drop a column
ALTER TABLE llap_native_orders REPLACE COLUMNS (orderid INT, quantity BIGINT, total DECIMAL(20,2), orderdate DATE, tradets TIMESTAMP, shipped BOOLEAN, product STRING, unitprice DECIMAL(7,2), region STRING);
SELECT orderid, product, unitprice, total FROM llap_native_orders WHERE orderid > 8 ORDER BY orderid;

--schema evolution: drop and re-add a column with the same name
ALTER TABLE llap_native_orders REPLACE COLUMNS (orderid INT, quantity BIGINT, total DECIMAL(20,2), orderdate DATE, tradets TIMESTAMP, shipped BOOLEAN, product STRING, region STRING);
ALTER TABLE llap_native_orders ADD COLUMNS (unitprice DECIMAL(7,2));
INSERT INTO llap_native_orders VALUES
(13, 5, 250000.00, date('2022-03-01'), timestamp('2022-03-01 10:00:00.000'), true, 'Cybertruck', 'US', 50000.00);
SELECT orderid, product, unitprice FROM llap_native_orders WHERE orderid > 10 ORDER BY orderid;
SELECT count(*), count(unitprice), sum(unitprice) FROM llap_native_orders;

-- position deletes are applied by HiveDeleteFilter on the native-cache batches; results must match the plain reader
DELETE FROM llap_native_orders WHERE orderid IN (2, 7, 12);
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
SELECT orderid, product, unitprice, total FROM llap_native_orders ORDER BY orderid;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter;
SELECT orderid, ROW__POSITION, PARTITION__NAME FROM llap_native_orders WHERE region = 'EU' ORDER BY orderid;
SELECT region, count(*), sum(quantity), sum(total) FROM llap_native_orders GROUP BY region ORDER BY region;
SELECT orderid, product, unitprice FROM llap_native_orders WHERE orderid > 10 ORDER BY orderid;

--re-read from the cache only
set hive.llap.io.cache.only=true;
SELECT region, count(*), sum(quantity), sum(total) FROM llap_native_orders GROUP BY region ORDER BY region;
SELECT orderid, product, unitprice FROM llap_native_orders WHERE orderid > 10 ORDER BY orderid;
set hive.llap.io.cache.only=false;

DROP TABLE llap_native_orders PURGE;
