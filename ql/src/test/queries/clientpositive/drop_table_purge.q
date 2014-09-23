SET hive.metastore.batch.retrieve.max=1;
CREATE TABLE IF NOT EXISTS temp(col STRING);

DROP TABLE temp PURGE;
