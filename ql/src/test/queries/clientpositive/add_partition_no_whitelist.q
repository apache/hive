SET hive.metastore.pre.event.listeners= ;
-- Test with PartitionNameWhitelistPreEventListener NOT registered

CREATE TABLE part_nowhitelist_test (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS part_nowhitelist_test;

ALTER TABLE part_nowhitelist_test ADD PARTITION (ds='1,2,3,4');
