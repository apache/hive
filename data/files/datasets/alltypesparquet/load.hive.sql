CREATE TABLE alltypesorc_to_parquet(
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
OVERWRITE INTO TABLE alltypesorc_to_parquet;

CREATE TABLE alltypesparquet(
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
    STORED AS PARQUET;

INSERT OVERWRITE TABLE alltypesparquet SELECT * FROM alltypesorc_to_parquet;

ANALYZE TABLE alltypesparquet COMPUTE STATISTICS;

ANALYZE TABLE alltypesparquet COMPUTE STATISTICS FOR COLUMNS ctinyint,csmallint,cint,cbigint,cfloat,cdouble,cstring1,cstring2,ctimestamp1,ctimestamp2,cboolean1,cboolean2;
DROP TABLE alltypesorc_to_parquet;
