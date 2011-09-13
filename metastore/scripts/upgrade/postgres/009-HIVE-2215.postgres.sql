SELECT '< HIVE-2215 Add api for marking querying set of partitions for events >';

-- Table "PARTITION_EVENTS" for classes [org.apache.hadoop.hive.metastore.model.MPartitionEvent]
CREATE TABLE "PARTITION_EVENTS"
(
    "PART_NAME_ID" int8 NOT NULL,
    "DB_NAME" varchar(128) NULL,
    "EVENT_TIME" int8 NOT NULL,
    "EVENT_TYPE" int4 NOT NULL,
    "PARTITION_NAME" varchar(767) NULL,
    "TBL_NAME" varchar(128) NULL,
    PRIMARY KEY ("PART_NAME_ID")
);

-- Constraints for table "PARTITION_EVENTS" for class(es) [org.apache.hadoop.hive.metastore.model.MPartitionEvent]
CREATE INDEX "PARTITIONEVENTINDEX" ON "PARTITION_EVENTS" ("PARTITION_NAME");


