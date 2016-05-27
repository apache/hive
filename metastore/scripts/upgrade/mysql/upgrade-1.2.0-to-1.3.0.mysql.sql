SELECT 'Upgrading MetaStore schema from 1.2.0 to 1.3.0' AS ' ';
SOURCE 021-HIVE-7018.mysql.sql;
SOURCE 022-HIVE-11970.mysql.sql;
SOURCE 023-HIVE-12807.mysql.sql;
SOURCE 024-HIVE-12814.mysql.sql;
SOURCE 025-HIVE-12816.mysql.sql;
SOURCE 026-HIVE-12818.mysql.sql;
SOURCE 027-HIVE-12819.mysql.sql;
SOURCE 028-HIVE-12821.mysql.sql;
SOURCE 029-HIVE-12822.mysql.sql;
SOURCE 030-HIVE-12823.mysql.sql;
SOURCE 031-HIVE-12831.mysql.sql;
SOURCE 032-HIVE-12832.mysql.sql;
SOURCE 035-HIVE-13395.mysql.sql;
SOURCE 036-HIVE-13354.mysql.sql;

UPDATE VERSION SET SCHEMA_VERSION='1.3.0', VERSION_COMMENT='Hive release version 1.3.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 1.2.0 to 1.3.0' AS ' ';
