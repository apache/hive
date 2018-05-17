--! qt:dataset:alltypesorc

SET hive.vectorized.execution.enabled=false;
CREATE TABLE druid_table_n0
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

 -- MATH AND STRING functions

SELECT count(*) FROM druid_table_n0 WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10 AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3;

SELECT count(*) FROM druid_table_n0 WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10;

SELECT count(*) FROM druid_table_n0 WHERE power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_n0 WHERE ceil(cfloat) > 0 AND floor(cdouble) * 2 < 1000 OR ln(cdouble) / log10(10) > 0 AND COS(cint) > 0 OR SIN(cdouble) > 1;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_n0 WHERE ceil(cfloat) > 0 AND floor(cdouble) * 2 < 1000;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_n0 WHERE  ln(cdouble) / log10(10) > 0 AND COS(cint) > 0 OR SIN(cdouble) > 1;

SELECT  SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
FROM druid_table_n0 WHERE  SIN(cdouble) > 1;

SELECT cstring1 || '_'|| cstring2, substring(cstring2, 2, 3) as concat , upper(cstring2), lower(cstring1), SUM(cdouble) as s FROM druid_table_n0 WHERE cstring1 IS NOT NULL AND cstring2 IS NOT NULL AND cstring2 like 'Y%'
 GROUP BY cstring1 || '_'|| cstring2, substring(cstring2, 2, 3), upper(cstring2), lower(cstring1) ORDER BY concat DESC LIMIT 10;

EXPLAIN SELECT count(*) FROM druid_table_n0 WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10 AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3;

EXPLAIN SELECT SUM(cfloat + 1), CAST(SUM(cdouble + ctinyint) AS INTEGER), SUM(ctinyint) + 1 , CAST(SUM(csmallint) + SUM(cint) AS DOUBLE), SUM(cint), SUM(cbigint)
        FROM druid_table_n0 WHERE ceil(cfloat) > 0 AND floor(cdouble) * 2 < 1000 OR ln(cdouble) / log10(10) > 0 AND COS(cint) > 0 OR SIN(cdouble) > 1;

EXPLAIN SELECT cstring1 || '_'|| cstring2, substring(cstring2, 2, 3) as concat , upper(cstring2), lower(cstring1), SUM(cdouble) as s FROM druid_table_n0 WHERE cstring1 IS NOT NULL AND cstring2 IS NOT NULL AND cstring2 like 'Y%'
         GROUP BY cstring1 || '_'|| cstring2, substring(cstring2, 2, 3), upper(cstring2), lower(cstring1) ORDER BY concat DESC LIMIT 10;

explain extended select count(*) from (select `__time` from druid_table_n0 limit 1) as src ;

SELECT `__time`
FROM druid_table_n0
WHERE (`__time` BETWEEN '1968-01-01 00:00:00' AND '1970-01-01 00:00:00')
    OR (`__time` BETWEEN '1968-02-01 00:00:00' AND '1970-04-01 00:00:00') ORDER BY `__time` ASC LIMIT 10;

-- COUNT DISTINCT TESTS
-- AS PART OF https://issues.apache.org/jira/browse/HIVE-19586

EXPLAIN select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_n0 GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(distinct cdouble), sum(cdouble) FROM druid_table_n0 GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(distinct cstring2), sum(2 * cdouble) FROM druid_table_n0 GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(distinct cstring2 || '_'|| cstring1), sum(cdouble) FROM druid_table_n0 GROUP  BY `__time`, `cstring1` ;

EXPLAIN select count(DISTINCT cstring2) FROM druid_table_n0 ;
EXPLAIN select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_n0 ;
EXPLAIN select count(distinct cstring2 || '_'|| cstring1), sum(cdouble), min(cint) FROM druid_table_n0;

select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_n0 GROUP  BY floor_year(`__time`) ;

select count(distinct cstring2), sum(2 * cdouble) FROM druid_table_n0 GROUP  BY floor_year(`__time`) ;

select count(DISTINCT cstring2) FROM druid_table_n0 ;

select count(DISTINCT cstring2), sum(cdouble) FROM druid_table_n0 ;

select count(distinct cstring2 || '_'|| cstring1), sum(cdouble), min(cint) FROM druid_table_n0;

DROP TABLE druid_table_n0;