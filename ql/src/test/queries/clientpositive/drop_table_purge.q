SET hive.metastore.batch.retrieve.max=1;
CREATE TABLE IF NOT EXISTS temp_n1(col STRING);

DROP TABLE temp_n1 PURGE;
