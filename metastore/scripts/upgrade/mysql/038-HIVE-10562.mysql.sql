-- Step 1: Add the column for format
ALTER TABLE `NOTIFICATION_LOG` ADD `MESSAGE_FORMAT` varchar(16);
-- if MESSAGE_FORMAT is null, then it is the legacy hcat JSONMessageFactory that created this message

-- Step 2 : Change the type of the MESSAGE field from mediumtext to longtext
ALTER TABLE `NOTIFICATION_LOG` MODIFY `MESSAGE` longtext;
