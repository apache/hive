SELECT '< HIVE-1068: CREATE VIEW followup: add a "table type" enum attribute in metastore >' AS ' ';
ALTER TABLE `TBLS` ADD `TBL_TYPE` VARCHAR(128);
