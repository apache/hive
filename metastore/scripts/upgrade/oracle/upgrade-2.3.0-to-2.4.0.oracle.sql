SELECT 'Upgrading MetaStore schema from 2.3.0 to 2.4.0' AS Status from dual;

@040-HIVE-16399.oracle.sql;
@041-HIVE-19372.oracle.sql;
@042-HIVE-19605.oracle.sql;

UPDATE VERSION SET SCHEMA_VERSION='2.4.0', VERSION_COMMENT='Hive release version 2.4.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 2.4.0' AS Status from dual;
