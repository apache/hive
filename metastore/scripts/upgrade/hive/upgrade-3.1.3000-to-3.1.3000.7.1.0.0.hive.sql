SELECT 'Upgrading MetaStore schema from 3.1.3000 to 3.1.3000.7.1.0.0';

USE SYS;

CREATE OR REPLACE VIEW `CDH_VERSION` AS SELECT 1 AS `VER_ID`, '3.1.3000.7.1.0.0' AS `SCHEMA_VERSION`,
  'Hive release version 3.1.3000 for CDH 7.1.0.0' AS `VERSION_COMMENT`;

SELECT 'Finished upgrading MetaStore schema from 3.1.3000 to 3.1.3000.7.1.0.0';
