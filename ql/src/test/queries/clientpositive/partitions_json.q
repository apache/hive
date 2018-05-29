set hive.ddl.output.format=json;

CREATE TABLE add_part_test_n0 (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS add_part_test_n0;

ALTER TABLE add_part_test_n0 ADD PARTITION (ds='2010-01-01');
SHOW PARTITIONS add_part_test_n0;

ALTER TABLE add_part_test_n0 ADD IF NOT EXISTS PARTITION (ds='2010-01-01');
SHOW PARTITIONS add_part_test_n0;

ALTER TABLE add_part_test_n0 ADD IF NOT EXISTS PARTITION (ds='2010-01-02');
SHOW PARTITIONS add_part_test_n0;

SHOW TABLE EXTENDED LIKE add_part_test_n0 PARTITION (ds='2010-01-02');

ALTER TABLE add_part_test_n0 DROP PARTITION (ds='2010-01-02');

DROP TABLE add_part_test_n0;

set hive.ddl.output.format=text;
