-- HIVE-28728: STR_TO_MAP() must preserve UTF-8 in vectorized execution when JVM default charset is not UTF-8.
-- Tez container opts below force US-ASCII in Tez tasks
-- Use driver-level mimic for testing with llap: -Dmaven.test.jvm.args="-Dfile.encoding=US-ASCII"

SET tez.am.launch.cmd-opts=-Dfile.encoding=US-ASCII;
SET hive.tez.java.opts=-Dfile.encoding=US-ASCII;
SET hive.vectorized.execution.enabled=true;
SET hive.fetch.task.conversion=none;

CREATE TABLE hive28728_src (id string, name string, multi string) STORED AS ORC;
INSERT INTO hive28728_src VALUES
  ('100','hive', 'en:1'),
  ('200','spark', null),
  ('300','oozie', 'a:1,b:2'),
  ('400','airflow', 'ascii:值'),
  ('500','优惠活动', '上海:北京,优惠活动:折扣'),
  ('600','日本語', 'val:1,val:2');

SELECT STR_TO_MAP(CONCAT(id, ':', name), ',', ':') FROM hive28728_src ORDER BY id;
SELECT STR_TO_MAP(multi, ',', ':') FROM hive28728_src WHERE multi IS NOT NULL ORDER BY id;
SELECT STR_TO_MAP(multi, ',', ':')['优惠活动'] FROM hive28728_src WHERE id = '500';
SELECT STR_TO_MAP('优惠活动:折扣,北京:海淀', ',', ':');

SELECT STR_TO_MAP(multi, ',', ':') FROM hive28728_src WHERE id = '200';
SELECT STR_TO_MAP('700', ',', ':');

-- Vectorized INSERT OVERWRITE
CREATE TABLE hive28728_result (cd MAP<STRING, STRING>) STORED AS ORC;
INSERT OVERWRITE TABLE hive28728_result
  SELECT STR_TO_MAP(CONCAT(id, ':', name), ',', ':') FROM hive28728_src;
SELECT * FROM hive28728_result ORDER BY cd;

CREATE TABLE hive28728_multi (cd MAP<STRING, STRING>) STORED AS ORC;
INSERT OVERWRITE TABLE hive28728_multi
  SELECT STR_TO_MAP(multi, ',', ':') FROM hive28728_src WHERE multi IS NOT NULL ORDER BY id;
SELECT * FROM hive28728_multi ORDER BY cd;

-- Non-vectorized baseline
SET hive.vectorized.execution.enabled=false;
CREATE TABLE hive28728_result_novec (cd MAP<STRING, STRING>) STORED AS ORC;
INSERT OVERWRITE TABLE hive28728_result_novec
  SELECT STR_TO_MAP(CONCAT(id, ':', name), ',', ':') FROM hive28728_src;
SELECT * FROM hive28728_result_novec ORDER BY cd;

SELECT STR_TO_MAP(CONCAT(id, ':', name), ',', ':') FROM hive28728_src ORDER BY id;
