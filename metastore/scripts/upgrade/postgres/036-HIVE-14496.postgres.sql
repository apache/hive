-- Step 1: Add the column allowing null
ALTER TABLE "TBLS" ADD COLUMN "IS_REWRITE_ENABLED" boolean NULL;

 -- Step 2: Replace the null with default value (false)
UPDATE "TBLS" SET "IS_REWRITE_ENABLED" = false;

-- Step 3: Alter the column to disallow null values
ALTER TABLE "TBLS" ALTER COLUMN "IS_REWRITE_ENABLED" SET NOT NULL;
