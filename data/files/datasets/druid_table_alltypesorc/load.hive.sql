CREATE TABLE alltypesorc1(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN)
    STORED AS ORC;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/alltypesorc"
OVERWRITE INTO TABLE alltypesorc1;

CREATE EXTERNAL TABLE druid_table_alltypesorc
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
  FROM alltypesorc1 where ctimestamp1 IS NOT NULL;
