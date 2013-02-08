SELECT '< HIVE-3649: Support stored as directories >' AS ' ';
-- Add new not null column into SDS table in three steps

-- Step 1: Add the column allowing null
ALTER TABLE `SDS` ADD `IS_STOREDASSUBDIRECTORIES` bit(1);

 -- Step 2: Replace the null with default value (false)
UPDATE `SDS` SET `IS_STOREDASSUBDIRECTORIES` = false;

-- Step 3: Alter the column to disallow null values
ALTER TABLE `SDS` MODIFY COLUMN `IS_STOREDASSUBDIRECTORIES` bit(1) NOT NULL;
