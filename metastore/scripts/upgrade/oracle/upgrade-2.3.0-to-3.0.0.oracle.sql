SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0' AS Status from dual;

@041-HIVE-16556.oracle.sql;
@042-HIVE-16575.oracle.sql;
@043-HIVE-16922.oracle.sql;
@044-HIVE-16997.oracle.sql;
@045-HIVE-16886.oracle.sql;
@046-HIVE-17566.oracle.sql;
@047-HIVE-18202-oracle.sql;
@048-HIVE-14498.oracle.sql;

UPDATE VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0' AS Status from dual;
