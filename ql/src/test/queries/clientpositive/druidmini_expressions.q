--! qt:disabled:HIVE-23984
--! qt:dataset:druid_table_alltypesorc
SET hive.ctas.external.tables=true;

SET hive.vectorized.execution.enabled=true;

 -- MATH AND STRING functions

SELECT count(*) FROM druid_table_alltypesorc WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10 AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3;

SELECT count(*) FROM druid_table_alltypesorc WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10;

SELECT count(*) FROM druid_table_alltypesorc WHERE power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_alltypesorc WHERE ceil(cfloat) > 0 AND floor(cdouble) * 2 < 1000 OR ln(cdouble) / log10(10) > 0 AND COS(cint) > 0 OR SIN(cdouble) > 1;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_alltypesorc WHERE ceil(cfloat) > 0 AND floor(cdouble) * 2 < 1000;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_alltypesorc WHERE  ln(cdouble) / log10(10) > 0 AND COS(cint) > 0 OR SIN(cdouble) > 1;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_alltypesorc WHERE  SIN(cdouble) > 1;

SELECT cstring1 || '_'|| cstring2, substring(cstring2, 2, 3) as concat , upper(cstring2), lower(cstring1), SUM(cdouble) as s FROM druid_table_alltypesorc WHERE cstring1 IS NOT NULL AND cstring2 IS NOT NULL AND cstring2 like 'Y%'
 GROUP BY cstring1 || '_'|| cstring2, substring(cstring2, 2, 3), upper(cstring2), lower(cstring1) ORDER BY concat DESC LIMIT 10;

EXPLAIN SELECT count(*) FROM druid_table_alltypesorc WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10 AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3;

EXPLAIN SELECT SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
        FROM druid_table_alltypesorc WHERE ceil(cfloat) > 0 AND floor(cdouble) * 2 < 1000 OR ln(cdouble) / log10(10) > 0 AND COS(cint) > 0 OR SIN(cdouble) > 1;

EXPLAIN SELECT cstring1 || '_'|| cstring2, substring(cstring2, 2, 3) as concat , upper(cstring2), lower(cstring1), SUM(cdouble) as s FROM druid_table_alltypesorc WHERE cstring1 IS NOT NULL AND cstring2 IS NOT NULL AND cstring2 like 'Y%'
         GROUP BY cstring1 || '_'|| cstring2, substring(cstring2, 2, 3), upper(cstring2), lower(cstring1) ORDER BY concat DESC LIMIT 10;

explain extended select count(*) from (select `__time` from druid_table_alltypesorc limit 1) as src ;



explain
SELECT `__time`
FROM druid_table_alltypesorc
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_alltypesorc
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;


explain
SELECT `__time`
FROM druid_table_alltypesorc
WHERE ('1968-01-01 00:00:00' <= `__time` AND `__time` <= '1970-01-01 00:00:00')
    OR ('1968-02-01 00:00:00' <= `__time` AND `__time` <= '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;

SELECT `__time`
FROM druid_table_alltypesorc
WHERE ('1968-01-01 00:00:00' <= `__time` AND `__time` <= '1970-01-01 00:00:00')
    OR ('1968-02-01 00:00:00' <= `__time` AND `__time` <= '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;

-- COUNT DISTINCT TESTS
-- AS PART OF https://issues.apache.org/jira/browse/HIVE-19586

EXPLAIN select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_alltypesorc GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(distinct cdouble), sum(cdouble) FROM druid_table_alltypesorc GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(distinct cstring2), sum(2 * cdouble) FROM druid_table_alltypesorc GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(distinct cstring2 || '_'|| cstring1), sum(cdouble) FROM druid_table_alltypesorc GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(DISTINCT cstring2) FROM druid_table_alltypesorc ;
EXPLAIN select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_alltypesorc ;
EXPLAIN select count(distinct cstring2 || '_'|| cstring1), sum(cdouble), min(cint) FROM druid_table_alltypesorc;

-- Force the scan query to test limit push down.
EXPLAIN select count(*) from (select `__time` from druid_table_alltypesorc limit 1025) as src;

select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_alltypesorc GROUP  BY floor_year(`__time`) ;

select count(distinct cstring2), sum(2 * cdouble) FROM druid_table_alltypesorc GROUP  BY floor_year(`__time`) ;

select count(DISTINCT cstring2) FROM druid_table_alltypesorc ;

explain select count(DISTINCT cstring1) FROM druid_table_alltypesorc ;

select count(DISTINCT cstring1) FROM druid_table_alltypesorc ;

select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_alltypesorc ;

select count(distinct cstring2 || '_'|| cstring1), sum(cdouble), min(cint) FROM druid_table_alltypesorc;

-- Force a scan query to test Vectorized path
select count(*) from (select `__time` from druid_table_alltypesorc limit 1025) as src;

-- Force a scan query to test Vectorized path
select count(*) from (select `__time` from druid_table_alltypesorc limit 200000) as src;

select count(`__time`) from (select `__time` from druid_table_alltypesorc limit 200) as src;

select count(distinct `__time`) from druid_table_alltypesorc;

select count(distinct `ctimestamp1`) from alltypesorc1  where ctimestamp1 IS NOT NULL;

explain select `timets` from (select `__time` as timets from druid_table_alltypesorc order by timets limit 10)  as src order by `timets`;

explain select `timets` from (select cast(`__time` as timestamp ) as timets from druid_table_alltypesorc order by timets limit 10)  as src order by `timets`;

select `timets_with_tz` from (select `__time` as timets_with_tz from druid_table_alltypesorc order by timets_with_tz limit 10)  as src order by `timets_with_tz`;

select `timets` from (select cast(`__time` as timestamp ) as timets from druid_table_alltypesorc order by timets limit 10)  as src order by `timets`;

explain select unix_timestamp(from_unixtime(1396681200)) from druid_table_alltypesorc limit 1;
select unix_timestamp(from_unixtime(1396681200)) from druid_table_alltypesorc limit 1;

explain select unix_timestamp(`__time`) from druid_table_alltypesorc limit 1;
select unix_timestamp(`__time`) from druid_table_alltypesorc limit 1;

explain select FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss' ),'yyyy-MM-dd HH:mm:ss')
from druid_table_alltypesorc
GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss' ),'yyyy-MM-dd HH:mm:ss');

select FROM_UNIXTIME(UNIX_TIMESTAMP (CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss' ),'yyyy-MM-dd HH:mm:ss')
from druid_table_alltypesorc
GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss' ),'yyyy-MM-dd HH:mm:ss');

explain select TRUNC(cast(`__time` as timestamp), 'YY') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'YY');
select TRUNC(cast(`__time` as timestamp), 'YY') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'YY');
select TRUNC(cast(`__time` as timestamp), 'YEAR') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'YEAR');
select TRUNC(cast(`__time` as timestamp), 'YYYY') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'YYYY');

explain select TRUNC(cast(`__time` as timestamp), 'MONTH') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'MONTH');
select TRUNC(cast(`__time` as timestamp), 'MONTH') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'MONTH');
select TRUNC(cast(`__time` as timestamp), 'MM') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'MM');
select TRUNC(cast(`__time` as timestamp), 'MON') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'MON');

explain select TRUNC(cast(`__time` as timestamp), 'QUARTER') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'QUARTER');
select TRUNC(cast(`__time` as timestamp), 'QUARTER') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'QUARTER');
select TRUNC(cast(`__time` as timestamp), 'Q') from druid_table_alltypesorc GROUP BY TRUNC(cast(`__time` as timestamp), 'Q');

explain select TO_DATE(`__time`) from druid_table_alltypesorc GROUP BY TO_DATE(`__time`);
select TO_DATE(`__time`) from druid_table_alltypesorc GROUP BY TO_DATE(`__time`);

EXPLAIN SELECT SUM((`druid_table_alias`.`cdouble` * `druid_table_alias`.`cdouble`)) AS `sum_calculation_4998925219892510720_ok`,
  CAST(TRUNC(CAST(`druid_table_alias`.`__time` AS TIMESTAMP),'MM') AS DATE) AS `tmn___time_ok`
FROM `default`.`druid_table_alltypesorc` `druid_table_alias`
GROUP BY CAST(TRUNC(CAST(`druid_table_alias`.`__time` AS TIMESTAMP),'MM') AS DATE);


SELECT SUM((`druid_table_alias`.`cdouble` * `druid_table_alias`.`cdouble`)) AS `sum_calculation_4998925219892510720_ok`,
  CAST(TRUNC(CAST(`druid_table_alias`.`__time` AS TIMESTAMP),'MM') AS DATE) AS `tmn___time_ok`
FROM `default`.`druid_table_alltypesorc` `druid_table_alias`
GROUP BY CAST(TRUNC(CAST(`druid_table_alias`.`__time` AS TIMESTAMP),'MM') AS DATE);

explain SELECT DATE_ADD(cast(`__time` as date), CAST((cdouble / 1000) AS INT)) as date_1,  DATE_SUB(cast(`__time` as date), CAST((cdouble / 1000) AS INT)) as date_2, cast(`__time` as date) as orig_date, CAST((cdouble / 1000) AS INT) as offset from druid_table_alltypesorc  order by date_1, date_2 limit 3;
SELECT DATE_ADD(cast(`__time` as date), CAST((cdouble / 1000) AS INT)) as date_1,  DATE_SUB(cast(`__time` as date), CAST((cdouble / 1000) AS INT)) as date_2, cast(`__time` as date) as orig_date, CAST((cdouble / 1000) AS INT) as offset from druid_table_alltypesorc  order by date_1, date_2 limit 3;

  -- Boolean Values
-- Expected results of this query are wrong due to https://issues.apache.org/jira/browse/CALCITE-2319
-- It should get fixed once we upgrade calcite
 EXPLAIN SELECT cboolean2, count(*) from druid_table_alltypesorc GROUP BY cboolean2;
 SELECT cboolean2, count(*) from druid_table_alltypesorc GROUP BY cboolean2;

  -- Expected results of this query are wrong due to https://issues.apache.org/jira/browse/CALCITE-2319
  -- It should get fixed once we upgrade calcite
 SELECT ctinyint > 2, count(*) from druid_table_alltypesorc GROUP BY ctinyint > 2;

 EXPLAIN SELECT ctinyint > 2, count(*) from druid_table_alltypesorc GROUP BY ctinyint > 2;

-- group by should be rewitten and pushed into druid
-- simple gby with single constant key
EXPLAIN SELECT sum(cfloat) FROM druid_table_alltypesorc WHERE cstring1 != 'en' group by 1.011;
SELECT sum(cfloat) FROM druid_table_alltypesorc WHERE cstring1 != 'en' group by 1.011;

-- gby with multiple constant keys
EXPLAIN SELECT sum(cfloat) FROM druid_table_alltypesorc WHERE cstring1 != 'en' group by 1.011, 3.40;
SELECT sum(cfloat) FROM druid_table_alltypesorc WHERE cstring1 != 'en' group by 1.011, 3.40;

-- group by with constant folded key
EXPLAIN SELECT sum(cint) FROM druid_table_alltypesorc WHERE cfloat= 0.011 group by cfloat;
SELECT sum(cint) FROM druid_table_alltypesorc WHERE cfloat= 0.011 group by cfloat;

-- group by key is referred in select
EXPLAIN SELECT cfloat, sum(cint) FROM druid_table_alltypesorc WHERE cfloat= 0.011 group by cfloat;
SELECT cfloat, sum(cint) FROM druid_table_alltypesorc WHERE cfloat= 0.011 group by cfloat;

-- Tests for testing handling of date/time funtions on druid dimensions stored as strings
CREATE TABLE druid_table_n1
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR", "druid.query.granularity" = "MINUTE")
AS
  SELECT cast (current_timestamp() as timestamp with local time zone) as `__time`,
cast(datetime1 as string) as datetime1,
cast(date1 as string) as date1,
cast(time1 as string) as time1
FROM TABLE (
VALUES
('2004-04-09 22:20:14', '2004-04-09','22:20:14'),
('2004-04-04 22:50:16', '2004-04-04', '22:50:16'),
('2004-04-12 04:40:49', '2004-04-12', '04:40:49'),
('2004-04-11 00:00:00', '2004-04-11', null),
('00:00:00 18:58:41', null, '18:58:41')) as q (datetime1, date1, time1);

EXPLAIN SELECT TO_DATE(date1), TO_DATE(datetime1) FROM druid_table_n1;

SELECT TO_DATE(date1), TO_DATE(datetime1) FROM druid_table_n1;

EXPLAIN select count(*) from (select `__time` from druid_table_alltypesorc limit 1025) as src;

select count(*) from (select `__time` from druid_table_alltypesorc limit 1025) as src;

-- No Vectorization since __time is timestamp with local time zone
explain select `timets` from (select `__time` as timets from druid_table_alltypesorc order by timets limit 10)  as src order by `timets`;
-- Vectorization is on now since we cast to Timestamp
explain select `timets` from (select cast(`__time` as timestamp ) as timets from druid_table_alltypesorc order by timets limit 10)  as src order by `timets`;

select `timets_with_tz` from (select `__time` as timets_with_tz from druid_table_alltypesorc order by timets_with_tz limit 10)  as src order by `timets_with_tz`;

select `timets` from (select cast(`__time` as timestamp ) as timets from druid_table_alltypesorc order by timets limit 10)  as src order by `timets`;

select count(cfloat) from (select `cfloat`, `cstring1` from druid_table_alltypesorc limit 1025) as src;

select count(cstring1) from (select `cfloat`, `cstring1` from druid_table_alltypesorc limit 90000) as src;

explain select count(cstring1) from (select `cfloat`, `cstring1`, `cint` from druid_table_alltypesorc limit 90000) as src;

select max(cint * cdouble) from (select `cfloat`, `cstring1`, `cint`, `cdouble` from druid_table_alltypesorc limit 90000) as src;

explain select max(cint * cfloat) from (select `cfloat`, `cstring1`, `cint`, `cdouble` from druid_table_alltypesorc limit 90000) as src;

explain select count(distinct `__time`, cint) from (select * from druid_table_alltypesorc) as src;

select count(distinct `__time`, cint) from (select * from druid_table_alltypesorc) as src;
