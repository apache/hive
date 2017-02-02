-- Step 1: Add the column allowing null
ALTER TABLE "APP"."TBLS" ADD "IS_REWRITE_ENABLED" CHAR(1);

 -- Step 2: Replace the null with default value (false)
UPDATE "APP"."TBLS" SET "IS_REWRITE_ENABLED" = 'N';

-- Step 3: Alter the column to disallow null values
ALTER TABLE "APP"."TBLS" ALTER COLUMN "IS_REWRITE_ENABLED" NOT NULL;
