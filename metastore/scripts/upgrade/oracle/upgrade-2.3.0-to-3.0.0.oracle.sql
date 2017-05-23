SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0' AS Status from dual;

@042-HIVE-16575.oracle.sql;

UPDATE VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0' AS Status from dual;
