set hive.stats.dbclass=fs;
--
-- Table alltypesorc
--
DROP TABLE IF EXISTS alltypesorc;
CREATE TABLE alltypesorc(
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
OVERWRITE INTO  TABLE alltypesorc;

ANALYZE TABLE alltypesorc COMPUTE STATISTICS;

ANALYZE TABLE alltypesorc COMPUTE STATISTICS FOR COLUMNS ctinyint,csmallint,cint,cbigint,cfloat,cdouble,cstring1,cstring2,ctimestamp1,ctimestamp2,cboolean1,cboolean2;

-- Druid Table

