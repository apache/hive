
CREATE TABLE "APP"."CTLGS" (
    "CTLG_ID" BIGINT NOT NULL,
    "NAME" VARCHAR(256) UNIQUE,
    "DESC" VARCHAR(4000),
    "LOCATION_URI" VARCHAR(4000) NOT NULL);

ALTER TABLE "APP"."CTLGS" ADD CONSTRAINT "CTLGS_PK" PRIMARY KEY ("CTLG_ID");

-- Insert a default value.  The location is TBD.  Hive will fix this when it starts
INSERT INTO "APP"."CTLGS" VALUES (1, 'hive', 'Default catalog for Hive', 'TBD');

-- Drop the unique index on DBS
DROP INDEX "APP"."UNIQUE_DATABASE";

-- Add the new column to the DBS table, can't put in the not null constraint yet
ALTER TABLE "APP"."DBS" ADD COLUMN "CTLG_NAME" VARCHAR(256);

-- Update all records in the DBS table to point to the Hive catalog
UPDATE "APP"."DBS" 
  SET "CTLG_NAME" = 'hive';

-- Add the not null constraint
ALTER TABLE "APP"."DBS" ALTER COLUMN "CTLG_NAME" NOT NULL;

-- Put back the unique index 
CREATE UNIQUE INDEX "APP"."UNIQUE_DATABASE" ON "APP"."DBS" ("NAME", "CTLG_NAME");

-- Add the foreign key
ALTER TABLE "APP"."DBS" ADD CONSTRAINT "DBS_FK1" FOREIGN KEY ("CTLG_NAME") REFERENCES "APP"."CTLGS" ("NAME") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- Add columns to table stats and part stats
ALTER TABLE "APP"."TAB_COL_STATS" ADD COLUMN "CAT_NAME" VARCHAR(256);
ALTER TABLE "APP"."PART_COL_STATS" ADD COLUMN "CAT_NAME" VARCHAR(256);

-- Set the existing column names to Hive
UPDATE "APP"."TAB_COL_STATS"
  SET "CAT_NAME" = 'hive';
UPDATE "APP"."PART_COL_STATS"
  SET "CAT_NAME" = 'hive';

-- Add the not null constraint
ALTER TABLE "APP"."TAB_COL_STATS" ALTER COLUMN "CAT_NAME" NOT NULL;
ALTER TABLE "APP"."PART_COL_STATS" ALTER COLUMN "CAT_NAME" NOT NULL;

-- Rebuild the index for Part col stats.  No such index for table stats, which seems weird
DROP INDEX "APP"."PCS_STATS_IDX";
CREATE INDEX "APP"."PCS_STATS_IDX" ON "APP"."PART_COL_STATS" ("CAT_NAME", "DB_NAME","TABLE_NAME","COLUMN_NAME","PARTITION_NAME");

-- Add column to partition events
ALTER TABLE "APP"."PARTITION_EVENTS" ADD COLUMN "CAT_NAME" VARCHAR(256);

-- Add column to notification log
ALTER TABLE "APP"."NOTIFICATION_LOG" ADD COLUMN "CAT_NAME" VARCHAR(256);
