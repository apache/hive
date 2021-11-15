set hive.ddl.output.format=json;

CREATE TEMPORARY TABLE add_part_test_n0_temp (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS add_part_test_n0_temp;

ALTER TABLE add_part_test_n0_temp ADD PARTITION (ds='2010-01-01');
SHOW PARTITIONS add_part_test_n0_temp;

ALTER TABLE add_part_test_n0_temp ADD IF NOT EXISTS PARTITION (ds='2010-01-01');
SHOW PARTITIONS add_part_test_n0_temp;

ALTER TABLE add_part_test_n0_temp ADD IF NOT EXISTS PARTITION (ds='2010-01-02');
SHOW PARTITIONS add_part_test_n0_temp;

SHOW TABLE EXTENDED LIKE add_part_test_n0_temp PARTITION (ds='2010-01-02');

ALTER TABLE add_part_test_n0_temp DROP PARTITION (ds='2010-01-02');

DROP TABLE add_part_test_n0_temp;

set hive.ddl.output.format=text;
