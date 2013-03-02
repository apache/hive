-- Add new not null column into SDS table in three steps

-- Step 1: Add the column allowing null
ALTER TABLE "SDS" ADD "IS_STOREDASSUBDIRECTORIES" CHAR(1);

 -- Step 2: Replace the null with default value (false)
UPDATE "SDS" SET "IS_STOREDASSUBDIRECTORIES" = 'N';

-- Step 3: Alter the column to disallow null values
ALTER TABLE "SDS" ALTER COLUMN "IS_STOREDASSUBDIRECTORIES" NOT NULL;
