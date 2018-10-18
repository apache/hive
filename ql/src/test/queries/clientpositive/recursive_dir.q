--! qt:dataset:src

CREATE TABLE fact_daily_n1(x int) PARTITIONED BY (ds STRING);
CREATE TABLE fact_tz_n0(x int) PARTITIONED BY (ds STRING, hr STRING)
LOCATION 'pfile:${system:test.tmp.dir}/fact_tz';

INSERT OVERWRITE TABLE fact_tz_n0 PARTITION (ds='1', hr='1')
SELECT key+11 FROM src WHERE key=484;

ALTER TABLE fact_daily_n1 SET TBLPROPERTIES('EXTERNAL'='TRUE');
ALTER TABLE fact_daily_n1 ADD PARTITION (ds='1')
LOCATION 'pfile:${system:test.tmp.dir}/fact_tz/ds=1';

set mapred.input.dir.recursive=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT * FROM fact_daily_n1 WHERE ds='1';

SELECT count(1) FROM fact_daily_n1 WHERE ds='1';
