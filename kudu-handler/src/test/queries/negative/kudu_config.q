-- unset the default master addresses config to validate handling.
set hive.kudu.master.addresses.default=;

CREATE EXTERNAL TABLE kv_table(key int, value string)
STORED BY 'org.apache.hadoop.hive.kudu.KuduStorageHandler'
TBLPROPERTIES ("kudu.table_name" = "default.kudu_kv");
