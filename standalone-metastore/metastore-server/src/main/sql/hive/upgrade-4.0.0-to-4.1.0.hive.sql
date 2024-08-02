SELECT 'Upgrading MetaStore schema from 4.0.0 to 4.1.0';

USE SYS;

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.1.0' AS `SCHEMA_VERSION`,
  'Hive release version 4.1.0' AS `VERSION_COMMENT`;

SELECT 'Finished upgrading MetaStore schema from 4.0.0 to 4.1.0';
