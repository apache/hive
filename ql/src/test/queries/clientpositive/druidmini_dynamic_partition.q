SET hive.vectorized.execution.enabled=false;
SET hive.ctas.external.tables=true;
SET hive.external.table.purge.default = true;
CREATE EXTERNAL TABLE druid_partitioned_table_0
        STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
        TBLPROPERTIES (
        "druid.segment.granularity" = "HOUR",
        "druid.query.granularity" = "MINUTE",
        "druid.segment.targetShardsPerGranularity" = "0"
        )
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

EXPLAIN CREATE EXTERNAL TABLE druid_partitioned_table
        STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
        TBLPROPERTIES (
        "druid.segment.granularity" = "HOUR",
        "druid.query.granularity" = "MINUTE",
        "druid.segment.targetShardsPerGranularity" = "6"
        )
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



CREATE EXTERNAL TABLE druid_partitioned_table
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
"druid.segment.granularity" = "HOUR",
"druid.query.granularity" = "MINUTE",
"druid.segment.targetShardsPerGranularity" = "6"
)
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
-- @FIXME https://issues.apache.org/jira/browse/HIVE-19011
-- SELECT  sum(cint), max(cbigint),  sum(cbigint), max(cint) FROM druid_partitioned_table;
-- SELECT  sum(cint), max(cbigint),  sum(cbigint), max(cint) FROM druid_partitioned_table_0;


SELECT  sum(cint), sum(cbigint) FROM druid_partitioned_table;
SELECT  sum(cint), sum(cbigint) FROM druid_partitioned_table_0;

SELECT floor_hour(cast(`ctimestamp1` as timestamp with local time zone)) as `__time`,
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
          FROM alltypesorc where ctimestamp1 IS NOT NULL order by `__time`, cstring2 DESC NULLS LAST, cstring1 DESC NULLS LAST LIMIT 10 ;


EXPLAIN INSERT INTO TABLE druid_partitioned_table
SELECT cast (`ctimestamp2` as timestamp with local time zone) as `__time`,
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
  FROM alltypesorc where ctimestamp2 IS NOT NULL;

INSERT INTO TABLE druid_partitioned_table
SELECT cast (`ctimestamp2` as timestamp with local time zone) as `__time`,
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
  FROM alltypesorc where ctimestamp2 IS NOT NULL;
-- @FIXME https://issues.apache.org/jira/browse/HIVE-19011
-- SELECT sum(cint), max(cbigint),  sum(cbigint), max(cint) FROM druid_partitioned_table;

SELECT sum(cint), sum(cbigint) FROM druid_partitioned_table;

EXPLAIN INSERT OVERWRITE TABLE druid_partitioned_table
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


INSERT OVERWRITE TABLE druid_partitioned_table
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

-- @FIXME https://issues.apache.org/jira/browse/HIVE-19011
--SELECT sum(cint), max(cbigint),  sum(cbigint), max(cint) FROM druid_partitioned_table ;
--SELECT  sum(cint), max(cbigint),  sum(cbigint), max(cint) FROM druid_partitioned_table_0;

SELECT sum(cint), sum(cbigint) FROM druid_partitioned_table ;

set hive.druid.indexer.partition.size.max=10;

CREATE EXTERNAL TABLE druid_max_size_partition
        STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
        TBLPROPERTIES (
        "druid.segment.granularity" = "HOUR",
        "druid.query.granularity" = "MINUTE"
        )
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

SELECT  sum(cint), sum(cbigint) FROM druid_max_size_partition ;

-- @FIXME https://issues.apache.org/jira/browse/HIVE-19011
--SELECT  sum(cint), max(cbigint),  sum(cbigint), max(cint)  FROM druid_max_size_partition ;
--SELECT  sum(cint), max(cbigint),  sum(cbigint), max(cint) FROM druid_partitioned_table_0;
--SELECT sum(cint), max(cbigint),  sum(cbigint), max(cint) FROM druid_partitioned_table ;

DROP TABLE druid_partitioned_table_0;
DROP TABLE druid_partitioned_table;
DROP TABLE druid_max_size_partition;