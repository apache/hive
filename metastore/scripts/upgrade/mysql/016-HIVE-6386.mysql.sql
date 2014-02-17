SELECT '< HIVE-6386: Add owner filed to database >' AS ' ';

ALTER TABLE `DBS` ADD `OWNER_NAME` varchar(128);
ALTER TABLE `DBS` ADD `OWNER_TYPE` varchar(10);

