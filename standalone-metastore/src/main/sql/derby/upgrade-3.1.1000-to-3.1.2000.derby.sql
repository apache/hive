SELECT 'Upgrading MetaStore schema from 3.1.1000 to 3.1.2000';

ALTER TABLE "TAB_COL_STATS"
ADD COLUMN "ENGINE" character varying(128) NOT NULL;

ALTER TABLE "PART_COL_STATS"
ADD COLUMN "ENGINE" character varying(128) NOT NULL;

-- These lines need to be last.  Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='3.1.2000', "VERSION_COMMENT"='Hive release version 3.1.2000' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.1000 to 3.1.2000';
