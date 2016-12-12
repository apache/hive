-- Step 1: Add the column allowing null
ALTER TABLE `TBLS` ADD `IS_REWRITE_ENABLED` bit(1);

 -- Step 2: Replace the null with default value (false)
UPDATE `TBLS` SET `IS_REWRITE_ENABLED` = false;

-- Step 3: Alter the column to disallow null values
ALTER TABLE `TBLS` MODIFY COLUMN `IS_REWRITE_ENABLED` bit(1) NOT NULL;
