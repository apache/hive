SELECT '< HIVE-2612 support hive table/partitions exists in more than one region >';

--
-- Table: REGION_SDS
--

CREATE TABLE "REGION_SDS" (
  "SD_ID" bigint NOT NULL,
  "REGION_NAME" character varying(512) NOT NULL,
  "LOCATION" character varying(4000) DEFAULT NULL,
  PRIMARY KEY ("SD_ID", "REGION_NAME")
);

--
-- Foreign Key Definitions
--

ALTER TABLE "REGION_SDS" ADD FOREIGN KEY ("SD_ID")
  REFERENCES "SDS" ("SD_ID") DEFERRABLE;

--
-- Alter Table: SDS
--

ALTER TABLE "SDS" ADD COLUMN "PRIMARY_REGION_NAME"
  character varying(512) NOT NULL DEFAULT '';
