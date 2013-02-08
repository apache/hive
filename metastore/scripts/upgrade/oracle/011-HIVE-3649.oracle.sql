-- Add new not null column into SDS table in three steps

-- Step 1: Add the column allowing null
ALTER TABLE  SDS ADD IS_STOREDASSUBDIRECTORIES NUMBER(1) NULL;

 -- Step 2: Replace the null with default value (false)
UPDATE SDS SET IS_STOREDASSUBDIRECTORIES = 0;

-- Step 3: Alter the column to disallow null values
ALTER TABLE SDS MODIFY(IS_STOREDASSUBDIRECTORIES NOT NULL);
