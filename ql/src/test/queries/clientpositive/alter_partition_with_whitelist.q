--! qt:dataset:part
SET hive.metastore.partition.name.whitelist.pattern=[A-Za-z]*;
-- This pattern matches only letters.

CREATE TABLE part_whitelist_test_n0 (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS part_whitelist_test_n0;

ALTER TABLE part_whitelist_test_n0 ADD PARTITION (ds='Part');

ALTER TABLE part_whitelist_test_n0 PARTITION (ds='Part') rename to partition (ds='Apart');     
