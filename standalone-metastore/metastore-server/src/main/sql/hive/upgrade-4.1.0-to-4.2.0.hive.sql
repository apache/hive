SELECT 'Upgrading MetaStore schema from 4.1.0 to 4.2.0';

-- HIVE-29123
DROP TABLE IF EXISTS `PROTO_HIVE_QUERY_DATA`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_HIVE_QUERY_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_QUERY_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents$HiveHookEventProto',
  'proto.maptypes'='org.apache.hadoop.hive.ql.hooks.proto.MapFieldEntry'
);

DROP TABLE IF EXISTS `PROTO_TEZ_APP_DATA`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_APP_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_APP_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);

DROP TABLE IF EXISTS `PROTO_TEZ_DAG_DATA`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);

DROP TABLE IF EXISTS `PROTO_TEZ_DAG_META`;
CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_META`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.LenientProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_META_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$ManifestEntryProto'
);

SELECT 'Finished upgrading MetaStore schema from 4.1.0 to 4.2.0';
