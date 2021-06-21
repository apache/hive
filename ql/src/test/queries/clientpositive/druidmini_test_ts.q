--! qt:disabled:HIVE-24816
--! qt:dataset:druid_table_alltypesorc

-- Time Series Query
SELECT count(*) FROM druid_table_alltypesorc;

SELECT floor_year(`__time`), SUM(cfloat), SUM(cdouble), SUM(ctinyint), SUM(csmallint),SUM(cint), SUM(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), MIN(cfloat), MIN(cdouble), MIN(ctinyint), MIN(csmallint),MIN(cint), MIN(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), MAX(cfloat), MAX(cdouble), MAX(ctinyint), MAX(csmallint),MAX(cint), MAX(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);


-- Group By

SELECT cstring1, SUM(cdouble) as s FROM druid_table_alltypesorc GROUP BY cstring1 ORDER BY s ASC LIMIT 10;

SELECT cstring2, MAX(cdouble) FROM druid_table_alltypesorc GROUP BY cstring2 ORDER BY cstring2 ASC LIMIT 10;


-- TIME STUFF

SELECT `__time`
FROM druid_table_alltypesorc ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` < '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00'
    AND `__time` < '2011-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;;

SELECT `__time`
FROM druid_table_alltypesorc
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;

-- (-∞‥+∞)
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc;

-- (-∞‥2012-03-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` < '2012-03-01 00:00:00';

-- [2010-01-01 00:00:00‥2012-03-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '2010-01-01 00:00:00' AND `__time` <= '2012-03-01 00:00:00';

-- [2010-01-01 00:00:00‥2011-01-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '2010-01-01 00:00:00' AND `__time` <= '2012-03-01 00:00:00'
    AND `__time` < '2011-01-01 00:00:00';

-- [2010-01-01 00:00:00‥2011-01-01 00:00:00]
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` BETWEEN '2010-01-01 00:00:00' AND '2011-01-01 00:00:00';

-- [2010-01-01 00:00:00‥2011-01-01 00:00:00],[2012-01-01 00:00:00‥2013-01-01 00:00:00]
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE (`__time` BETWEEN '2010-01-01 00:00:00' AND '2011-01-01 00:00:00')
    OR (`__time` BETWEEN '2012-01-01 00:00:00' AND '2013-01-01 00:00:00');

-- OVERLAP [2010-01-01 00:00:00‥2012-01-01 00:00:00]
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE (`__time` BETWEEN '2010-01-01 00:00:00' AND '2011-01-01 00:00:00')
    OR (`__time` BETWEEN '2010-06-01 00:00:00' AND '2012-01-01 00:00:00');

-- IN: MULTIPLE INTERVALS [2010-01-01 00:00:00‥2010-01-01 00:00:00),[2011-01-01 00:00:00‥2011-01-01 00:00:00)
EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` IN ('2010-01-01 00:00:00','2011-01-01 00:00:00');

EXPLAIN
SELECT `__time`, cstring2
FROM druid_table_alltypesorc
WHERE cstring2 = 'user1' AND `__time` IN ('2010-01-01 00:00:00','2011-01-01 00:00:00');

EXPLAIN
SELECT `__time`, cstring2
FROM druid_table_alltypesorc
WHERE cstring2 = 'user1' OR `__time` IN ('2010-01-01 00:00:00','2011-01-01 00:00:00');
