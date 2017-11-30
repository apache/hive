set hive.stats.dbclass=fs;
--
-- Table src
--
DROP TABLE IF EXISTS src;

CREATE TABLE src (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" INTO TABLE src;

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
-- Table src_json
--
DROP TABLE IF EXISTS src_json;

CREATE TABLE src_json (json STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/json.txt" INTO TABLE src_json;

ANALYZE TABLE src_json COMPUTE STATISTICS;

ANALYZE TABLE src_json COMPUTE STATISTICS FOR COLUMNS json;

--
-- Table src_sequencefile
--
DROP TABLE IF EXISTS src_sequencefile;

CREATE TABLE src_sequencefile (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.seq" INTO TABLE src_sequencefile;

ANALYZE TABLE src_sequencefile COMPUTE STATISTICS;

ANALYZE TABLE src_sequencefile COMPUTE STATISTICS FOR COLUMNS key,value;

--
-- Table src_thrift
--
DROP TABLE IF EXISTS src_thrift;

CREATE TABLE src_thrift
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
WITH SERDEPROPERTIES (
  'serialization.class' = 'org.apache.hadoop.hive.serde2.thrift.test.Complex',
  'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol')
STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/complex.seq" INTO TABLE src_thrift;

ANALYZE TABLE src_thrift COMPUTE STATISTICS;

--
-- Table srcbucket
--
DROP TABLE IF EXISTS srcbucket_tmp;
DROP TABLE IF EXISTS srcbucket;

CREATE TABLE srcbucket_tmp (key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE srcbucket (key INT, value STRING)
CLUSTERED BY (key) INTO 2 BUCKETS
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket0.txt" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket1.txt" INTO TABLE srcbucket_tmp;
INSERT INTO srcbucket SELECT * FROM srcbucket_tmp;
DROP TABLE srcbucket_tmp;
 
ANALYZE TABLE srcbucket COMPUTE STATISTICS;

ANALYZE TABLE srcbucket COMPUTE STATISTICS FOR COLUMNS key,value;

--
-- Table srcbucket2
--
DROP TABLE IF EXISTS srcbucket_tmp;
DROP TABLE IF EXISTS srcbucket2;

CREATE TABLE srcbucket_tmp (key INT, value STRING);
CREATE TABLE srcbucket2 (key INT, value STRING)
CLUSTERED BY (key) INTO 4 BUCKETS
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket20.txt" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket21.txt" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket22.txt" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/srcbucket23.txt" INTO TABLE srcbucket_tmp;
INSERT INTO srcbucket2 SELECT * FROM srcbucket_tmp;
DROP TABLE srcbucket_tmp;

ANALYZE TABLE srcbucket2 COMPUTE STATISTICS;

ANALYZE TABLE srcbucket2 COMPUTE STATISTICS FOR COLUMNS key,value;

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

--
-- Table primitives
--
DROP TABLE IF EXISTS primitives;
CREATE TABLE primitives (
  id INT COMMENT 'default',
  bool_col BOOLEAN COMMENT 'default',
  tinyint_col TINYINT COMMENT 'default',
  smallint_col SMALLINT COMMENT 'default',
  int_col INT COMMENT 'default',
  bigint_col BIGINT COMMENT 'default',
  float_col FLOAT COMMENT 'default',
  double_col DOUBLE COMMENT 'default',
  date_string_col STRING COMMENT 'default',
  string_col STRING COMMENT 'default',
  timestamp_col TIMESTAMP COMMENT 'default')
PARTITIONED BY (year INT COMMENT 'default', month INT COMMENT 'default')
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090101.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=1);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090201.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=2);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090301.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=3);

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/types/primitives/090401.txt"
OVERWRITE INTO TABLE primitives PARTITION(year=2009, month=4);

--
-- Function qtest_get_java_boolean
--
DROP FUNCTION IF EXISTS qtest_get_java_boolean;
CREATE FUNCTION qtest_get_java_boolean AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaBoolean';

--
-- Table dest1
--
DROP TABLE IF EXISTS dest1;

CREATE TABLE dest1 (key STRING COMMENT 'default', value STRING COMMENT 'default')
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

--
-- Table dest2
--
DROP TABLE IF EXISTS dest2;

CREATE TABLE dest2 (key STRING COMMENT 'default', value STRING COMMENT 'default')
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

--
-- Table dest3
--
DROP TABLE IF EXISTS dest3;

CREATE TABLE dest3 (key STRING COMMENT 'default', value STRING COMMENT 'default')
PARTITIONED BY (ds STRING, hr STRING)
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
ALTER TABLE dest3 ADD PARTITION (ds='2008-04-08',hr='12');

--
-- Table dest4
--
DROP TABLE IF EXISTS dest4;

CREATE TABLE dest4 (key STRING COMMENT 'default', value STRING COMMENT 'default')
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

--
-- Table dest4_sequencefile
--
DROP TABLE IF EXISTS dest4_sequencefile;

CREATE TABLE dest4_sequencefile (key STRING COMMENT 'default', value STRING COMMENT 'default')
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat';


--
-- CBO tables
--

drop table if exists cbo_t1;
drop table if exists cbo_t2;
drop table if exists cbo_t3;
drop table if exists src_cbo;
drop table if exists part;
drop table if exists lineitem;

set hive.cbo.enable=true;

create table cbo_t1(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table cbo_t2(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table cbo_t3(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table cbo_t1 partition (dt='2014');
load data local inpath '../../data/files/cbo_t2.txt' into table cbo_t2 partition (dt='2014');
load data local inpath '../../data/files/cbo_t3.txt' into table cbo_t3;

CREATE TABLE part(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
);

LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table part;

CREATE TABLE lineitem (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE lineitem;

create table src_cbo as select * from src;


analyze table cbo_t1 partition (dt) compute statistics;
analyze table cbo_t1 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table cbo_t2 partition (dt) compute statistics;
analyze table cbo_t2 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table cbo_t3 compute statistics;
analyze table cbo_t3 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table src_cbo compute statistics;
analyze table src_cbo compute statistics for columns;
analyze table part compute statistics;
analyze table part compute statistics for columns;
analyze table lineitem compute statistics;
analyze table lineitem compute statistics for columns;

reset;
set hive.stats.dbclass=fs;
