SELECT 'Upgrading MetaStore schema from 4.2.0 to 4.3.0' AS Status from dual;

ALTER TABLE HIVE_LOCKS ADD (HL_CATALOG VARCHAR2(128) DEFAULT 'hive' NOT NULL);
ALTER TABLE MATERIALIZATION_REBUILD_LOCKS ADD (MRL_CAT_NAME VARCHAR2(128) DEFAULT 'hive' NOT NULL);

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='4.3.0', VERSION_COMMENT='Hive release version 4.3.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 4.2.0 to 4.3.0' AS Status from dual;
