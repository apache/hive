SELECT 'Upgrading MetaStore schema from 3.1.0 to 4.0.0';

USE SYS;

DROP TABLE IF EXISTS `VERSION`;

CREATE OR REPLACE VIEW `VERSION` AS SELECT 1 AS `VER_ID`, '4.0.0' AS `SCHEMA_VERSION`,
  'Hive release version 4.0.0' AS `VERSION_COMMENT`;

SELECT 'Finished upgrading MetaStore schema from 3.1.0 to 4.0.0';
