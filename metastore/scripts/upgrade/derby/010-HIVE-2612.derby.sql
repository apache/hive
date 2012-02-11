/*
 * HIVE-2612 support hive table/partitions exists in more than one region
 */

/*
 * Creates the following table:
 *  - REGION_SDS
 */
CREATE TABLE "REGION_SDS" (
  "SD_ID" bigint NOT NULL,
  "REGION_NAME" varchar(512) NOT NULL,
  "LOCATION" varchar(4000),
  PRIMARY KEY ("SD_ID", "REGION_NAME")
);

ALTER TABLE "REGION_SDS" 
  ADD CONSTRAINT "REGION_SDS_FK1"
  FOREIGN KEY ("SD_ID") REFERENCES "SDS" ("SD_ID")
  ON DELETE NO ACTION ON UPDATE NO ACTION
;

/* Alter the SDS table to:
 *  - add the column PRIMARY_REGION_NAME
 */
ALTER TABLE SDS
  ADD COLUMN PRIMARY_REGION_NAME varchar(512) NOT NULL DEFAULT ''
;
