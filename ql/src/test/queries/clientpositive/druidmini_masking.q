set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
SET hive.ctas.external.tables=true;
SET hive.external.table.purge.default = true;

CREATE EXTERNAL TABLE masking_test_druid
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR")
AS
  SELECT cast(current_timestamp() AS timestamp with local time zone) AS `__time`,
  cast(username AS string) AS username,
  cast(double1 AS double) AS double1,
  cast(key AS int) AS key
  FROM TABLE (
  VALUES
  ('alfred', 10.30, -2),
  ('bob', 3.14, null),
  ('bonnie', null, 100),
  ('calvin', null, null),
  ('charlie', 15.8, 20)) as q (username, double1, key);

explain select username, key from masking_test_druid;

select username, key from masking_test_druid;
