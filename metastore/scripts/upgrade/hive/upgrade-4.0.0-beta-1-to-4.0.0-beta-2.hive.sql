SELECT 'Upgrading MetaStore schema from 4.0.0-beta-1 to 4.0.0-beta-2';

USE SYS;

CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_HIVE_QUERY_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_QUERY_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents$HiveHookEventProto',
  'proto.maptypes'='org.apache.hadoop.hive.ql.hooks.proto.MapFieldEntry'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_APP_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_APP_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_DATA`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_DATA_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$HistoryEventProto',
  'proto.maptypes'='KVPair'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `PROTO_TEZ_DAG_META`
PARTITIONED BY (
  `date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '_REPLACE_WITH_DAG_META_LOCATION_'
TBLPROPERTIES (
  'proto.class'='org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos$ManifestEntryProto'
);

DROP TABLE IF EXISTS `VERSION`;

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.0.0-beta-2' AS `SCHEMA_VERSION`,
                                           'Hive release version 4.0.0-beta-2' AS `VERSION_COMMENT`;

SELECT 'Finished upgrading MetaStore schema from 4.0.0-beta-1 to 4.0.0-beta-2';
