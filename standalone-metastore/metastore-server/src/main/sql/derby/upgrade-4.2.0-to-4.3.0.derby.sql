ALTER TABLE HIVE_LOCKS ADD COLUMN HL_CATALOG varchar(128) NOT NULL DEFAULT 'hive';
ALTER TABLE MATERIALIZATION_REBUILD_LOCKS ADD COLUMN MRL_CAT_NAME varchar(128) NOT NULL DEFAULT 'hive';

-- This needs to be the last thing done.  Insert any changes above this line.
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.3.0', VERSION_COMMENT='Hive release version 4.3.0' where VER_ID=1;
