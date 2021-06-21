--! qt:disabled:HIVE-24816
--! qt:dataset:druid_table_alltypesorc
SET hive.ctas.external.tables=true;
SET hive.vectorized.execution.enabled=true;
SET hive.external.table.purge.default = true;

-- Time Series Query
explain select count(*) FROM druid_table_alltypesorc;
SELECT count(*) FROM druid_table_alltypesorc;


EXPLAIN SELECT floor_year(`__time`), SUM(cfloat), SUM(cdouble), SUM(ctinyint), SUM(csmallint),SUM(cint), SUM(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), SUM(cfloat), SUM(cdouble), SUM(ctinyint), SUM(csmallint),SUM(cint), SUM(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);

EXPLAIN SELECT floor_year(`__time`), MIN(cfloat), MIN(cdouble), MIN(ctinyint), MIN(csmallint),MIN(cint), MIN(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), MIN(cfloat), MIN(cdouble), MIN(ctinyint), MIN(csmallint),MIN(cint), MIN(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);


EXPLAIN SELECT floor_year(`__time`), MAX(cfloat), MAX(cdouble), MAX(ctinyint), MAX(csmallint),MAX(cint), MAX(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);

SELECT floor_year(`__time`), MAX(cfloat), MAX(cdouble), MAX(ctinyint), MAX(csmallint),MAX(cint), MAX(cbigint)
FROM druid_table_alltypesorc GROUP BY floor_year(`__time`);


-- Group By


EXPLAIN SELECT cstring1, SUM(cdouble) as s FROM druid_table_alltypesorc GROUP BY cstring1 ORDER BY s ASC LIMIT 10;

SELECT cstring1, SUM(cdouble) as s FROM druid_table_alltypesorc GROUP BY cstring1 ORDER BY s ASC LIMIT 10;


EXPLAIN SELECT cstring2, MAX(cdouble) FROM druid_table_alltypesorc GROUP BY cstring2 ORDER BY cstring2 ASC LIMIT 10;

SELECT cstring2, MAX(cdouble) FROM druid_table_alltypesorc GROUP BY cstring2 ORDER BY cstring2 ASC LIMIT 10;


-- TIME STUFF

EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_alltypesorc ORDER BY `__time` ASC LIMIT 10;

EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` < '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` < '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00'
    AND `__time` < '2011-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` >= '1968-01-01 00:00:00' AND `__time` <= '1970-03-01 00:00:00'
    AND `__time` < '2011-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;


EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;;


SELECT `__time`
FROM druid_table_alltypesorc
WHERE `__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00' ORDER BY `__time` ASC LIMIT 10;;


EXPLAIN
SELECT `__time`
FROM druid_table_alltypesorc
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;


SELECT `__time`
FROM druid_table_alltypesorc
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;

-- Running this against Druid  will if Druid version does not include
-- this patch https://github.com/druid-io/druid/commit/219e77aeac9b07dc20dd9ab2dd537f3f17498346

explain select (cstring1 is null ) AS is_null, (cint is not null ) as isnotnull FROM druid_table_alltypesorc;

explain select substring(to_date(`__time`), 4) from druid_table_alltypesorc limit 5;
select substring(to_date(`__time`), 4) from druid_table_alltypesorc limit 5;

explain select substring(cast(to_date(`__time`) as string), 4) from druid_table_alltypesorc limit 5;
select substring(cast(to_date(`__time`) as string), 4) from druid_table_alltypesorc limit 5;

