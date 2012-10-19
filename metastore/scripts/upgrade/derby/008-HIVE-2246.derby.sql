/*
 * Creates the following tables:
 *  - CDS
 *  - COLUMNS_V2
 * The new columns table is called COLUMNS_V2
 * because many columns are removed, and the schema is changed.
 * It'd take too long to migrate and keep the same table.
 */
CREATE TABLE "CDS" (
  "CD_ID" bigint NOT NULL,
  PRIMARY KEY ("CD_ID")
);

CREATE TABLE "COLUMNS_V2" (
  "CD_ID" bigint NOT NULL,
  "COMMENT" varchar(4000),
  "COLUMN_NAME" varchar(128) NOT NULL,
  "TYPE_NAME" varchar(4000),
  "INTEGER_IDX" INTEGER NOT NULL,
  PRIMARY KEY ("CD_ID", "COLUMN_NAME")
);

ALTER TABLE "COLUMNS_V2" 
  ADD CONSTRAINT "COLUMNS_V2_FK1"
  FOREIGN KEY ("CD_ID") REFERENCES "CDS" ("CD_ID")
  ON DELETE NO ACTION ON UPDATE NO ACTION
;

/* Alter the SDS table to:
 *  - add the column CD_ID
 *  - add a foreign key on CD_ID
 *  - create an index on CD_ID
 */ 
ALTER TABLE SDS
  ADD COLUMN "CD_ID" bigint
;
ALTER TABLE SDS
  ADD CONSTRAINT "SDS_FK2"
  FOREIGN KEY ("CD_ID") REFERENCES "CDS" ("CD_ID")
;

/*
 * Migrate the TBLS table
 * Add entries into CDS.
 * Populate the CD_ID field in SDS for tables
 * Add entires to COLUMNS_V2 based on this table's sd's columns
 */ 

/* In the migration, there is a 1:1 mapping between CD_ID and SD_ID
 * for tables. For speed, just let CD_ID = SD_ID for tables 
 */
INSERT INTO CDS (CD_ID)
SELECT t.SD_ID FROM TBLS t WHERE t.SD_ID IS NOT NULL;

UPDATE SDS
  SET CD_ID = SD_ID
WHERE SD_ID in 
(SELECT t.SD_ID FROM TBLS t WHERE t.SD_ID IS NOT NULL);

INSERT INTO COLUMNS_V2
  (CD_ID, COMMENT, COLUMN_NAME, TYPE_NAME, INTEGER_IDX)
SELECT 
  c.SD_ID, c.COMMENT, c.COLUMN_NAME, c.TYPE_NAME, c.INTEGER_IDX
FROM
  COLUMNS c
JOIN
  TBLS t
ON
  t.SD_ID = c.SD_ID
;

/*
 * Migrate the partitions.
 * Update the partitions' SDS to use the parent tables' CD_ID  BEGIN
 * Derby does not allow joins in update statements, 
 * so we have to make a temporary tableh
 */
DECLARE GLOBAL TEMPORARY TABLE "TMP_TBL" (
  "SD_ID" bigint not null,
  "CD_ID" bigint not null
) ON COMMIT PRESERVE ROWS NOT LOGGED;

INSERT INTO "SESSION"."TMP_TBL" SELECT
  p.SD_ID, sds.CD_ID
  FROM PARTITIONS p
  JOIN TBLS t ON t.TBL_ID = p.TBL_ID
  JOIN SDS sds on t.SD_ID = sds.SD_ID
  WHERE p.SD_ID IS NOT NULL AND sds.CD_ID IS NOT NULL;

UPDATE SDS sd
  SET sd.CD_ID = 
    (SELECT tt.CD_ID FROM SESSION.TMP_TBL tt WHERE tt.SD_ID = sd.SD_ID)
  WHERE sd.SD_ID IN (SELECT SD_ID FROM SESSION.TMP_TBL);

/*
 * Migrate IDXS
 */
INSERT INTO CDS (CD_ID)
SELECT i.SD_ID FROM IDXS i WHERE i.SD_ID IS NOT NULL;

UPDATE SDS
  SET CD_ID = SD_ID
WHERE SD_ID in 
(SELECT i.SD_ID FROM IDXS i WHERE i.SD_ID IS NOT NULL);

INSERT INTO COLUMNS_V2
  (CD_ID, COMMENT, COLUMN_NAME, TYPE_NAME, INTEGER_IDX)
SELECT 
  c.SD_ID, c.COMMENT, c.COLUMN_NAME, c.TYPE_NAME, c.INTEGER_IDX
FROM
  COLUMNS c
JOIN
  IDXS i
ON
  i.SD_ID = c.SD_ID
;

/*
 * rename the old COLUMNS table
 */
RENAME TABLE COLUMNS TO COLUMNS_OLD;
