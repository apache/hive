SELECT 'Upgrading MetaStore schema from 3.1.1000 to 3.1.2000' AS Status from dual;

ALTER TABLE TAB_COL_STATS
ADD ENGINE character varying(128);

ALTER TABLE PART_COL_STATS
ADD ENGINE character varying(128);

--- Migrate Impala statistics from CDH to CDP. The TAB_COL_STATS table records
--- will be duplicated and the ENGINE field filled.
---
--- 1) Create a temporary table for Impala
CREATE TABLE TMP_TAB_COL_STATS AS SELECT * FROM TAB_COL_STATS;

--- 2) Create a sequence to increment the CS_ID column values
CREATE SEQUENCE mtablecolumnstatisticsseq START WITH 1 INCREMENT BY 1;

--- 3) Update the unique ID, ENGINE and number of nulls field
UPDATE TMP_TAB_COL_STATS
SET CS_ID=mtablecolumnstatisticsseq.nextval +
  (SELECT NEXT_VAL
    FROM SEQUENCE_TABLE
    WHERE SEQUENCE_NAME = 'org.apache.hadoop.hive.metastore.model.MTableColumnStatistics');
UPDATE TAB_COL_STATS SET ENGINE = 'hive' WHERE ENGINE IS NULL;
UPDATE PART_COL_STATS SET ENGINE = 'hive' WHERE ENGINE IS NULL;
UPDATE TMP_TAB_COL_STATS SET ENGINE = 'impala' WHERE ENGINE IS NULL;

--- 4) Ingest the values back to TAB_COL_STATS
INSERT INTO TAB_COL_STATS SELECT * FROM TMP_TAB_COL_STATS;

--- 5) Update the MTableColumnStatistics with new counter
UPDATE SEQUENCE_TABLE
SET NEXT_VAL = NEXT_VAL + mtablecolumnstatisticsseq.nextval
WHERE SEQUENCE_NAME = 'org.apache.hadoop.hive.metastore.model.MTableColumnStatistics';

--- 6) Drop the temporary table
DROP TABLE TMP_TAB_COL_STATS;
DROP SEQUENCE mtablecolumnstatisticsseq;

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.1.2000', VERSION_COMMENT='Hive release version 3.1.2000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.1000 to 3.1.2000' AS Status from dual;
