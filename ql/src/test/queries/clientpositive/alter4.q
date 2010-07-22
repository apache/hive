CREATE TABLE set_bucketing_test (key INT, value STRING) CLUSTERED BY (key) INTO 10 BUCKETS;
DESCRIBE EXTENDED set_bucketing_test;

ALTER TABLE set_bucketing_test NOT CLUSTERED;
DESCRIBE EXTENDED set_bucketing_test;
