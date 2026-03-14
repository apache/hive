SELECT 'Upgrading MetaStore schema from  4.2.0 to 4.3.0' AS MESSAGE;

ALTER TABLE HIVE_LOCKS ADD HL_CATALOG nvarchar(128) NOT NULL DEFAULT 'hive';
ALTER TABLE MATERIALIZATION_REBUILD_LOCKS ADD MRL_CAT_NAME nvarchar(128) NOT NULL DEFAULT 'hive';

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='4.3.0', VERSION_COMMENT='Hive release version 4.3.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 4.2.0 to 4.3.0' AS MESSAGE;
