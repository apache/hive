--! qt:disabled:unstable resultset HIVE-23694

SET hive.vectorized.execution.enabled=true ;
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

!curl --noproxy * -ss http://localhost:8081/druid/indexer/v1/supervisor;

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


EXPLAIN
SELECT language, -1 * (a + b) AS c
FROM (
  SELECT (sum(added)-sum(deleted)) / (count(*) * 3) AS a, sum(deleted) AS b, language
  FROM druid_kafka_test
  GROUP BY language) subq
ORDER BY c DESC;

EXPLAIN
SELECT language, `user`, sum(added) - sum(deleted) AS a
FROM druid_kafka_test
WHERE extract (week from `__time`) IN (10,11)
GROUP BY language, `user`;

EXPLAIN
SELECT language, sum(deleted) / count(*) AS a
FROM druid_kafka_test
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT language, sum(added) / sum(deleted) AS a,
       CASE WHEN sum(deleted)=0 THEN 1.0 ELSE sum(deleted) END AS b
FROM druid_kafka_test
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT language, a, a - b as c
FROM (
  SELECT language, sum(added) + 100 AS a, sum(deleted) AS b
  FROM druid_kafka_test
  GROUP BY language) subq
ORDER BY a DESC;

EXPLAIN
SELECT language, `user`, "A"
FROM (
  SELECT sum(added) - sum(deleted) AS a, language, `user`
  FROM druid_kafka_test
  GROUP BY language, `user` ) subq
ORDER BY "A"
LIMIT 5;

EXPLAIN
SELECT language, `user`, "A"
FROM (
  SELECT language, sum(added) + sum(deleted) AS a, `user`
  FROM druid_kafka_test
  GROUP BY language, `user`) subq
ORDER BY `user`, language
LIMIT 5;

DROP TABLE druid_kafka_test;
