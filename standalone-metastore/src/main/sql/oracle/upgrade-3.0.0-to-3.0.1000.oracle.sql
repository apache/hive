-- HIVE-21077
ALTER TABLE DBS add CREATE_TIME NUMBER(10);
ALTER TABLE CTLGS add CREATE_TIME NUMBER(10);

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.0.1000', VERSION_COMMENT='Hive release version 3.0.1000' where VER_ID=1;
