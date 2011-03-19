-- HIVE-1068: CREATE VIEW followup: add a 'table type' enum attribute in metastore
ALTER TABLE "TBLS" ADD COLUMN "TBL_TYPE" VARCHAR(128);
