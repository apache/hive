SELECT 'Upgrading MetaStore schema from 3.1.1000 to 3.1.2000';

--- HIVE-22046
ALTER TABLE `TAB_COL_STATS`
ADD COLUMN `ENGINE` character varying(128);

ALTER TABLE `PART_COL_STATS`
ADD COLUMN `ENGINE` character varying(128);

--- Migrate Impala statistics from CDH to CDP. The TAB_COL_STATS table records
--- will be duplicated and the ENGINE field filled.
---
--- 1) Create a temporary table for Impala
CREATE TABLE tmp_TAB_COL_STATS AS SELECT * FROM TAB_COL_STATS;

--- 2) Get where the DataNucleus datastore-identity counter's value
SELECT NEXT_VAL
FROM SEQUENCE_TABLE
WHERE SEQUENCE_NAME = "org.apache.hadoop.hive.metastore.model.MTableColumnStatistics"
INTO @nextMTableColumnStatistics;

--- 3) Update the unique ID, ENGINE and number of nulls field
UPDATE tmp_TAB_COL_STATS
SET CS_ID = @nextMTableColumnStatistics := (@nextMTableColumnStatistics + 1);
UPDATE TAB_COL_STATS SET ENGINE = 'hive' WHERE ENGINE IS NULL;
UPDATE PART_COL_STATS SET ENGINE = 'hive' WHERE ENGINE IS NULL;
UPDATE tmp_TAB_COL_STATS SET ENGINE = 'impala' WHERE ENGINE IS NULL;

--- 4) Ingest the values back to TAB_COL_STATS
INSERT INTO TAB_COL_STATS SELECT * FROM tmp_TAB_COL_STATS;

--- 5) Update the MTableColumnStatistics with new counter
UPDATE SEQUENCE_TABLE
SET NEXT_VAL = @nextMTableColumnStatistics + 1
WHERE SEQUENCE_NAME = "org.apache.hadoop.hive.metastore.model.MTableColumnStatistics";

--- 6) Drop the temporary table
DROP TABLE tmp_TAB_COL_STATS;

-- These lines need to be last. Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.1.2000', VERSION_COMMENT='Hive release version 3.1.2000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.1000 to 3.1.2000';
