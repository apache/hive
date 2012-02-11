SELECT '< HIVE-2612 support hive table/partitions exists in more than one region >' AS ' ';

DELIMITER $$
DROP PROCEDURE IF EXISTS REVERT $$
DROP PROCEDURE IF EXISTS ALTER_SDS $$
DROP PROCEDURE IF EXISTS CREATE_TABLE $$
DROP PROCEDURE IF EXISTS MIGRATE $$

/* Call this procedure to revert all changes by this script */
CREATE PROCEDURE REVERT()
  BEGIN
    ALTER TABLE SDS
      DROP COLUMN PRIMARY_REGION_NAME
    ;
    DROP TABLE IF EXISTS REGION_SDS;

  END $$

/* Alter the SDS table to:
 *  - add the column PRIMARY_REGION_NAME
 */
CREATE PROCEDURE ALTER_SDS()
  BEGIN
    ALTER TABLE SDS
      ADD COLUMN PRIMARY_REGION_NAME varchar(512) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT ''
    ;
  END $$

/*
 * Creates the following table:
 *  - REGION_SDS
 */
CREATE PROCEDURE CREATE_TABLE()
  BEGIN
    CREATE TABLE IF NOT EXISTS `REGION_SDS` (
      SD_ID bigint(20) NOT NULL,
      REGION_NAME varchar(512) NOT NULL,
      LOCATION varchar(4000),
      PRIMARY KEY (`SD_ID`, `REGION_NAME`),
      KEY `REGION_SDS_N49` (`SD_ID`),
      CONSTRAINT `REGION_SDS_V2_FK1` FOREIGN KEY (`SD_ID`) REFERENCES `SDS` (`SD_ID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;
  END $$

/*
 * Procedures called before migration happens
 */
CREATE PROCEDURE MIGRATE()
  BEGIN
    call CREATE_TABLE();
    SELECT 'Created table REGION_SDS';
    call ALTER_SDS();
    SELECT 'Altered the SDS table';
  END $$

DELIMITER ;

CALL MIGRATE();
