set hive.metastore.transactional.event.listeners=org.apache.hive.hcatalog.listener.DbNotificationListener;
set metastore.jdbc.max.batch.size=10;

set hive.stats.autogather=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.reliable=true;

DROP TABLE IF EXISTS repro_batch_test;

CREATE TABLE repro_batch_test (
    id INT,
    name STRING
)
PARTITIONED BY (part_key INT);

INSERT INTO TABLE repro_batch_test PARTITION (part_key)
SELECT
    val as id,
    'dummy_data' as name,
    val as part_key
FROM (
    SELECT (a.pos + 1) as val
    FROM (SELECT posexplode(split(repeat(',', 10), ','))) a
) dummy;
