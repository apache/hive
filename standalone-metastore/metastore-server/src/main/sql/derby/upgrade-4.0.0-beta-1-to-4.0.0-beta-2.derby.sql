-- This needs to be the last thing done.  Insert any changes above this line.

--HIVE-27767
ALTER TABLE HIVE_LOCKS ADD HL_ERROR_MESSAGE varchar(4000);
UPDATE "APP".VERSION SET SCHEMA_VERSION='4.0.0-beta-2', VERSION_COMMENT='Hive release version 4.0.0-beta-2' where VER_ID=1;
