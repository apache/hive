--! qt:dataset:alltypesorc
CREATE TABLE druid_table_n3
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR", "druid.query.granularity" = "MINUTE")
AS
SELECT cast (`ctimestamp1` as timestamp with local time zone) as `__time`,
  cstring1,
  cstring2,
  cdouble,
  cfloat,
  ctinyint,
  csmallint,
  cint,
  cbigint,
  cboolean1,
  cboolean2
  FROM alltypesorc where ctimestamp1 IS NOT NULL;

-- Time Series Query
explain select count(*) FROM druid_table_n3;
SELECT count(*) FROM druid_table_n3;


EXPLAIN SELECT floor_year(`__time`), SUM(cfloat), SUM(cdouble), SUM(ctinyint), SUM(csmallint),SUM(cint), SUM(cbigint)
FROM druid_table_n3 GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), SUM(cfloat), SUM(cdouble), SUM(ctinyint), SUM(csmallint),SUM(cint), SUM(cbigint)
FROM druid_table_n3 GROUP BY floor_year(`__time`);

EXPLAIN SELECT floor_year(`__time`), MIN(cfloat), MIN(cdouble), MIN(ctinyint), MIN(csmallint),MIN(cint), MIN(cbigint)
FROM druid_table_n3 GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), MIN(cfloat), MIN(cdouble), MIN(ctinyint), MIN(csmallint),MIN(cint), MIN(cbigint)
FROM druid_table_n3 GROUP BY floor_year(`__time`);


EXPLAIN SELECT floor_year(`__time`), MAX(cfloat), MAX(cdouble), MAX(ctinyint), MAX(csmallint),MAX(cint), MAX(cbigint)
FROM druid_table_n3 GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), MAX(cfloat), MAX(cdouble), MAX(ctinyint), MAX(csmallint),MAX(cint), MAX(cbigint)
FROM druid_table_n3 GROUP BY floor_year(`__time`);


-- Group By


EXPLAIN SELECT cstring1, SUM(cdouble) as s FROM druid_table_n3 GROUP BY cstring1 ORDER BY s ASC LIMIT 10;

SELECT cstring1, SUM(cdouble) as s FROM druid_table_n3 GROUP BY cstring1 ORDER BY s ASC LIMIT 10;


EXPLAIN SELECT cstring2, MAX(cdouble) FROM druid_table_n3 GROUP BY cstring2 ORDER BY cstring2 ASC LIMIT 10;

SELECT cstring2, MAX(cdouble) FROM druid_table_n3 GROUP BY cstring2 ORDER BY cstring2 ASC LIMIT 10;


-- TIME STUFF

EXPLAIN
SELECT `__time`
FROM druid_table_n3 ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_n3 ORDER BY `__time` ASC LIMIT 10;

EXPLAIN
SELECT `__time`
FROM druid_table_n3
WHERE `__time` < '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_n3
WHERE `__time` < '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


EXPLAIN
SELECT `__time`
FROM druid_table_n3
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_n3
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


EXPLAIN
SELECT `__time`
FROM druid_table_n3
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00'
    AND `__time` < '2011-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_n3
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00'
    AND `__time` < '2011-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


EXPLAIN
SELECT `__time`
FROM druid_table_n3
WHERE `__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;;


SELECT `__time`
FROM druid_table_n3
WHERE `__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;;


EXPLAIN
SELECT `__time`
FROM druid_table_n3
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_n3
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;
