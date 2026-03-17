SELECT 'Upgrading MetaStore schema from 4.2.0 to 4.3.0';

ALTER TABLE `HIVE_LOCKS` ADD COLUMNS (`HL_CATALOG` string);

SELECT 'Finished upgrading MetaStore schema from 4.2.0 to 4.3.0';
