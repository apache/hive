SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.2.0' AS Status from dual;

@037-HIVE-14496.oracle.sql;
@038-HIVE-10562.oracle.sql;
@039-HIVE-12274.oracle.sql;

UPDATE VERSION SET SCHEMA_VERSION='2.2.0', VERSION_COMMENT='Hive release version 2.2.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.2.0' AS Status from dual;
