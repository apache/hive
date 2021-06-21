--! qt:disabled:HIVE-24816
SET hive.vectorized.execution.enabled=false;

CREATE EXTERNAL TABLE druid_kafka_test_avro(`__time` timestamp , `page` string, `user` string, `language` string,
                                            `country` string,`continent` string, `namespace` string, `newPage` boolean, `unpatrolled` boolean,
                                            `anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint)
        STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
        TBLPROPERTIES (
        "druid.segment.granularity" = "MONTH",
        "druid.query.granularity" = "MINUTE",
        "kafka.bootstrap.servers" = "localhost:9092",
        "kafka.topic" = "wiki_kafka_avro_table",
        "druid.kafka.ingestion.useEarliestOffset" = "true",
        "druid.kafka.ingestion.maxRowsInMemory" = "5",
        "druid.kafka.ingestion.startDelay" = "PT1S",
        "druid.kafka.ingestion.taskDuration" = "PT30S",
        "druid.kafka.ingestion.period" = "PT5S",
        "druid.kafka.ingestion.consumer.retries" = "2",
        "druid.kafka.ingestion.reportParseExceptions" = "true",
        "druid.timestamp.column" = "timestamp",
        "druid.timestamp.format" = "MM/dd/yyyy HH:mm:ss",
        "druid.parseSpec.format" = "avro",
        'avro.schema.literal'='{
          "type" : "record",
          "name" : "Wikipedia",
          "namespace" : "org.apache.hive.kafka",
          "version": "1",
          "fields" : [ {
            "name" : "isrobot",
            "type" : "boolean"
          }, {
            "name" : "channel",
            "type" : "string"
          }, {
            "name" : "timestamp",
            "type" : "string"
          }, {
            "name" : "flags",
            "type" : "string"
          }, {
            "name" : "isunpatrolled",
            "type" : "boolean"
          }, {
            "name" : "page",
            "type" : "string"
          }, {
            "name" : "diffurl",
            "type" : "string"
          }, {
            "name" : "added",
            "type" : "long"
          }, {
            "name" : "comment",
            "type" : "string"
          }, {
            "name" : "commentlength",
            "type" : "long"
          }, {
            "name" : "isnew",
            "type" : "boolean"
          }, {
            "name" : "isminor",
            "type" : "boolean"
          }, {
            "name" : "delta",
            "type" : "long"
          }, {
            "name" : "isanonymous",
            "type" : "boolean"
          }, {
            "name" : "user",
            "type" : "string"
          }, {
            "name" : "deltabucket",
            "type" : "double"
          }, {
            "name" : "deleted",
            "type" : "long"
          }, {
            "name" : "namespace",
            "type" : "string"
          } ]
        }'
        );

ALTER TABLE druid_kafka_test_avro SET TBLPROPERTIES('druid.kafka.ingestion' = 'START');

!curl --noproxy * -ss http://localhost:8081/druid/indexer/v1/supervisor;

-- Sleep for some time for ingestion tasks to ingest events
!sleep 60;

DESCRIBE druid_kafka_test_avro;
DESCRIBE EXTENDED druid_kafka_test_avro;

Select count(*) FROM druid_kafka_test_avro;

Select page FROM druid_kafka_test_avro;

DROP TABLE druid_kafka_test_avro;
