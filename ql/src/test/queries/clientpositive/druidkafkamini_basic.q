SET hive.vectorized.execution.enabled=false;
CREATE EXTERNAL TABLE druid_kafka_test(`__time` timestamp, page string, `user` string, language string, added int, deleted int)
        STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
        TBLPROPERTIES (
        "druid.segment.granularity" = "MONTH",
        "druid.query.granularity" = "MINUTE",
        "kafka.bootstrap.servers" = "localhost:9092",
        "kafka.topic" = "test-topic",
        "druid.kafka.ingestion.useEarliestOffset" = "true",
        "druid.kafka.ingestion.maxRowsInMemory" = "5",
        "druid.kafka.ingestion.startDelay" = "PT1S",
        "druid.kafka.ingestion.taskDuration" = "PT60S",
        "druid.kafka.ingestion.period" = "PT1S",
        "druid.kafka.ingestion.consumer.retries" = "2"
        );

ALTER TABLE druid_kafka_test SET TBLPROPERTIES('druid.kafka.ingestion' = 'START');

!curl -ss http://localhost:8081/druid/indexer/v1/supervisor;

-- Sleep for some time for ingestion tasks to ingest events
!sleep 60;

DESCRIBE druid_kafka_test;
DESCRIBE EXTENDED druid_kafka_test;

Select count(*) FROM druid_kafka_test;

Select page FROM druid_kafka_test order by page;

-- Reset kafka Ingestion, this would reset the offsets and since we are using useEarliestOffset,
-- We will see records duplicated after successful reset.
ALTER TABLE druid_kafka_test SET TBLPROPERTIES('druid.kafka.ingestion' = 'RESET');

-- Sleep for some time for ingestion tasks to ingest events
!sleep 60;

DESCRIBE druid_kafka_test;
DESCRIBE EXTENDED druid_kafka_test;

Select count(*) FROM druid_kafka_test;

Select page FROM druid_kafka_test order by page;

-- Join against other normal tables
CREATE TABLE languages(shortname string, fullname string);

INSERT INTO languages values
("en", "english"),
("ru", "russian");

EXPLAIN EXTENDED
SELECT a.fullname, b.`user`
FROM
(
(SELECT fullname, shortname
FROM languages) a
JOIN
(SELECT language, `user`
FROM druid_kafka_test) b
  ON a.shortname = b.language
);

SELECT a.fullname, b.`user`
FROM
(
(SELECT fullname, shortname
FROM languages) a
JOIN
(SELECT language, `user`
FROM druid_kafka_test) b
  ON a.shortname = b.language
) order by b.`user`;

DROP TABLE druid_kafka_test;
