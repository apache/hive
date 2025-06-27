SELECT 'Upgrading MetaStore schema from 4.1.0 to 4.2.0' AS MESSAGE;

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='4.2.0', VERSION_COMMENT='Hive release version 4.2.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 4.1.0 to 4.2.0' AS MESSAGE;
