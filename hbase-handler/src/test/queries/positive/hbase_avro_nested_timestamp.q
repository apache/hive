dfs -cp ${system:hive.root}data/files/nested_ts.avsc ${system:test.tmp.dir}/nested_ts.avsc;

CREATE EXTERNAL TABLE hbase_avro_table(
`key` string COMMENT '',
`data_frv4` struct<`id`:string, `dischargedate`:struct<`value`:timestamp>>)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
'serialization.format'='1',
'hbase.columns.mapping' = ':key,data:frV4',
'data.frV4.serialization.type'='avro',
'data.frV4.avro.schema.url'='${system:test.tmp.dir}/nested_ts.avsc'
)
TBLPROPERTIES (
'hbase.table.name' = 'HiveAvroTable',
'hbase.struct.autogenerate'='true');

set hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;

select data_frV4.dischargedate.value from hbase_avro_table;
