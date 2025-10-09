--! qt:dataset:src
set hive.strict.timestamp.conversion=false;

DROP TABLE hbase_table;
CREATE EXTERNAL TABLE hbase_table (key string, value string, `time` timestamp)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string,:timestamp")
  TBLPROPERTIES ("external.table.purge" = "true");
DESC extended hbase_table;
FROM src INSERT OVERWRITE TABLE hbase_table SELECT key, value, "2012-02-23 10:14:52" WHERE (key % 17) = 0;
SELECT * FROM hbase_table;

DROP TABLE hbase_table;
CREATE EXTERNAL TABLE hbase_table (key string, value string, `time` bigint)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string,:timestamp")
  TBLPROPERTIES ("external.table.purge" = "true");
FROM src INSERT OVERWRITE TABLE hbase_table SELECT key, value, 1329959754000 WHERE (key % 17) = 0;
SELECT key, value, cast(`time` as timestamp) FROM hbase_table;

DROP TABLE hbase_table;
CREATE EXTERNAL TABLE hbase_table (key string, value string, `time` bigint)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string,:timestamp")
  TBLPROPERTIES ("external.table.purge" = "true");
insert overwrite table hbase_table select key,value,ts FROM
(
  select key, value, 100000000000 as ts from src WHERE (key % 33) = 0
  UNION ALL
  select key, value, 200000000000 as ts from src WHERE (key % 37) = 0
) T;

explain
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` < 200000000000;
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` < 200000000000;

explain
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` > 100000000000;
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` > 100000000000;

explain
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` <= 100000000000;
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` <= 100000000000;

explain
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` >= 200000000000;
SELECT key, value, cast(`time` as timestamp) FROM hbase_table WHERE key > 100 AND key < 400 AND `time` >= 200000000000;

DROP TABLE hbase_table;
CREATE EXTERNAL TABLE hbase_table(key string, value map<string, string>, `time` timestamp)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:,:timestamp")
TBLPROPERTIES ("external.table.purge" = "true");
FROM src INSERT OVERWRITE TABLE hbase_table SELECT key, MAP("name", CONCAT(value, " Jr")), "2012-02-23 10:14:52" WHERE (key % 17) = 0;
FROM src INSERT INTO TABLE hbase_table SELECT key, MAP("age", '40'), "2015-12-12 12:12:12" WHERE (key % 17) = 0;
FROM src INSERT INTO TABLE hbase_table SELECT key, MAP("name", value), "2000-01-01 01:01:01" WHERE (key % 17) = 0;
SELECT * FROM hbase_table;
