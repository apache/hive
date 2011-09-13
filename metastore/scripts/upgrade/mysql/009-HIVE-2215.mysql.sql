-- Table `PARTITION_EVENTS` for classes [org.apache.hadoop.hive.metastore.model.MPartitionEvent]
SELECT '< HIVE-2215 Add api for marking  querying set of partitions for events >' AS ' ';

CREATE TABLE IF NOT EXISTS `PARTITION_EVENTS`
(
    `PART_NAME_ID` BIGINT NOT NULL,
    `DB_NAME` VARCHAR(128) BINARY NULL,
    `EVENT_TIME` BIGINT NOT NULL,
    `EVENT_TYPE` INTEGER NOT NULL,
    `PARTITION_NAME` VARCHAR(767) BINARY NULL,
    `TBL_NAME` VARCHAR(128) BINARY NULL,
    PRIMARY KEY (`PART_NAME_ID`)
) ENGINE=INNODB DEFAULT CHARSET=latin1;

-- Constraints for table `PARTITION_EVENTS` for class(es) [org.apache.hadoop.hive.metastore.model.MPartitionEvent]
CREATE INDEX `PARTITIONEVENTINDEX` ON `PARTITION_EVENTS` (`PARTITION_NAME`);


