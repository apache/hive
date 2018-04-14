set hive.stats.dbclass=fs;

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

reset;
set hive.stats.dbclass=fs;
