--! qt:dataset:src
set hive.lock.numretries=5;
set hive.lock.sleep.between.retries=5;
set hive.support.concurrency=true;

DROP TABLE insert_into1_neg;

CREATE TABLE insert_into1_neg (key int, value string);

LOCK TABLE insert_into1_neg SHARED;
INSERT INTO TABLE insert_into1_neg SELECT * FROM src LIMIT 100;

DROP TABLE insert_into1_neg;
