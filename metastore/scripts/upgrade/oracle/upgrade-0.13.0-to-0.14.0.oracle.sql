SELECT 'Upgrading MetaStore schema from 0.13.0 to 0.14.0' AS Status from dual;

@019-HIVE-7118.oracle.sql;
@020-HIVE-7784.oracle.sql;

UPDATE VERSION SET SCHEMA_VERSION='0.14.0', VERSION_COMMENT='Hive release version 0.14.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 0.13.0 to 0.14.0' AS Status from dual;
