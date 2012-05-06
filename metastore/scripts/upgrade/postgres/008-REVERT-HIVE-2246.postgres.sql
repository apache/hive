-- A postgreSQL script to revert Hive Metastore changes made by HIVE-2246
--
-- Remove the CD_ID column from SDS
-- Delete the CDS table
-- Delete the COLUMNS_V2 table
-- Rename the COLUMNS_OLD table to COLUMNS
--
SELECT '< Revert HIVE-2246: Dedupe tables column schemas from partitions in the metastore db >';

ALTER TABLE "SDS" DROP COLUMN "CD_ID";

DROP TABLE "COLUMNS_V2";

DROP TABLE "CDS";

ALTER TABLE "COLUMNS_OLD" RENAME TO "COLUMNS";

SELECT '< Revert Done >';
