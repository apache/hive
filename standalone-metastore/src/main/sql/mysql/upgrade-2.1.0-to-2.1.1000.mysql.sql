SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS ' ';

-- SOURCE 038-HIVE-10562.mysql.sql;
-- Step 1: Add the column for format
ALTER TABLE `NOTIFICATION_LOG` ADD `MESSAGE_FORMAT` varchar(16);
-- if MESSAGE_FORMAT is null, then it is the legacy hcat JSONMessageFactory that created this message

-- Step 2 : Change the type of the MESSAGE field from mediumtext to longtext
ALTER TABLE `NOTIFICATION_LOG` MODIFY `MESSAGE` longtext;

UPDATE VERSION SET SCHEMA_VERSION='2.1.1000', VERSION_COMMENT='Hive release version 2.1.1000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS ' ';

