--! qt:disabled:this started falling after the druid upgrade just like HIVE-23450
SET hive.vectorized.execution.enabled=false;

CREATE EXTERNAL TABLE druid_kafka_test_delimited(`__time` timestamp , `page` string, `user` string, `language` string,
                                            `country` string,`continent` string, `namespace` string, `newpage` boolean, `unpatrolled` boolean,
                                            `anonymous` boolean, `robot` boolean, added int, deleted int, delta bigint)
        STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
        TBLPROPERTIES (
        "druid.segment.granularity" = "MONTH",
        "druid.query.granularity" = "MINUTE",
        "kafka.bootstrap.servers" = "localhost:9092",
        "kafka.topic" = "wiki_kafka_csv",
        "druid.kafka.ingestion.useEarliestOffset" = "true",
        "druid.kafka.ingestion.maxRowsInMemory" = "5",
        "druid.kafka.ingestion.startDelay" = "PT1S",
        "druid.kafka.ingestion.taskDuration" = "PT30S",
        "druid.kafka.ingestion.period" = "PT5S",
        "druid.kafka.ingestion.consumer.retries" = "2",
        "druid.kafka.ingestion.reportParseExceptions" = "true",
        "druid.parseSpec.format" = "delimited",
        "druid.parseSpec.columns" = "__time,page,language,user,unpatrolled,newpage,robot,anonymous,namespace,continent,country,region,city,added,deleted,delta",
        "druid.parseSpec.delimiter"=","
        );

ALTER TABLE druid_kafka_test_delimited SET TBLPROPERTIES('druid.kafka.ingestion' = 'START');

!curl --noproxy * -ss http://localhost:8081/druid/indexer/v1/supervisor;

-- Sleep for some time for ingestion tasks to ingest events
!sleep 60;

DESCRIBE druid_kafka_test_delimited;
DESCRIBE EXTENDED druid_kafka_test_delimited;

Select count(*) FROM druid_kafka_test_delimited;

Select page FROM druid_kafka_test_delimited;

DROP TABLE druid_kafka_test_delimited;
