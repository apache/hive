--! qt:dataset:alltypesorc
SET hive.ctas.external.tables=true;
SET hive.external.table.purge.default = true;
CREATE EXTERNAL TABLE druid_alltypesorc_n0
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR", "druid.query.granularity" = "MINUTE")
AS
  SELECT cast (`ctimestamp2` as timestamp with local time zone) as `__time`,
cstring1,
cdouble,
cfloat,
ctinyint,
csmallint,
cint,
cbigint,
cboolean1
FROM alltypesorc where ctimestamp2 IS NOT NULL;

DESCRIBE druid_alltypesorc_n0;

DESCRIBE extended druid_alltypesorc_n0;

SELECT COUNT(*) FROM druid_alltypesorc_n0;

ALTER TABLE druid_alltypesorc_n0 ADD COLUMNS (cstring2 string, cboolean2 boolean, cint2 int);

DESCRIBE druid_alltypesorc_n0;

DESCRIBE extended druid_alltypesorc_n0;

SELECT COUNT(*) FROM druid_alltypesorc_n0 WHERE cstring2 IS NOT NULL;

INSERT INTO TABLE druid_alltypesorc_n0
  SELECT cast (`ctimestamp1` as timestamp with local time zone) as `__time`,
cstring1,
cdouble,
cfloat,
ctinyint,
csmallint,
cint,
cbigint,
cboolean1,
cstring2,
cboolean2,
cint
FROM alltypesorc where ctimestamp1 IS NOT NULL;


SELECT COUNT(*) FROM druid_alltypesorc_n0;

SELECT COUNT(*) FROM druid_alltypesorc_n0 WHERE cstring2 IS NULL;

SELECT COUNT(*) FROM druid_alltypesorc_n0 WHERE cstring2 IS NOT NULL;

DROP TABLE druid_alltypesorc_n0;
