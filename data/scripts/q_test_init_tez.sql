set hive.stats.dbclass=fs;

--
-- Table src
--
DROP TABLE IF EXISTS src;

CREATE TABLE src(key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" OVERWRITE INTO TABLE src;

ANALYZE TABLE src COMPUTE STATISTICS;

ANALYZE TABLE src COMPUTE STATISTICS FOR COLUMNS key,value;

--
-- Table src1
--
DROP TABLE IF EXISTS src1;

CREATE TABLE src1 (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv3.txt" INTO TABLE src1;

ANALYZE TABLE src1 COMPUTE STATISTICS;

ANALYZE TABLE src1 COMPUTE STATISTICS FOR COLUMNS key,value;

--
-- Table srcpart
--
DROP TABLE IF EXISTS srcpart;

CREATE TABLE srcpart (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-08", hr="11");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-08", hr="12");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-09", hr="11");

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt"
OVERWRITE INTO TABLE srcpart PARTITION (ds="2008-04-09", hr="12");

ANALYZE TABLE srcpart PARTITION(ds, hr) COMPUTE STATISTICS;

ANALYZE TABLE srcpart PARTITION(ds, hr) COMPUTE STATISTICS FOR COLUMNS key,value;

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
