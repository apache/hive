SELECT 'Upgrading MetaStore schema from 3.1.0 to 3.2.0' AS MESSAGE;

-- HIVE-19267
CREATE TABLE TXN_WRITE_NOTIFICATION_LOG (
  WNL_ID bigint NOT NULL,
  WNL_TXNID bigint NOT NULL,
  WNL_WRITEID bigint NOT NULL,
  WNL_DATABASE nvarchar(128) NOT NULL,
  WNL_TABLE nvarchar(128) NOT NULL,
  WNL_PARTITION nvarchar(767) NOT NULL,
  WNL_TABLE_OBJ text NOT NULL,
  WNL_PARTITION_OBJ text,
  WNL_FILES text,
  WNL_EVENT_TIME int NOT NULL
);
ALTER TABLE TXN_WRITE_NOTIFICATION_LOG ADD CONSTRAINT TXN_WRITE_NOTIFICATION_LOG_PK PRIMARY KEY (WNL_TXNID, WNL_DATABASE, WNL_TABLE, WNL_PARTITION);
INSERT INTO SEQUENCE_TABLE (SEQUENCE_NAME, NEXT_VAL) VALUES ('org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog', 1);


-- HIVE-20221

-- We can not change the datatype of a column with default value. Hence we first drop the default constraint
-- and then change the datatype. We wrap the code to drop the default constraint in a stored procedure to avoid
-- code duplicate. We create temporary stored procedures since we do not need them during normal
-- metastore operation.
CREATE PROCEDURE #DROP_DEFAULT_CONSTRAINT @TBL_NAME sysname, @COL_NAME sysname
AS
BEGIN
	DECLARE @constraintname sysname
	SELECT @constraintname = default_constraints.name
		FROM sys.all_columns INNER JOIN sys.tables ON all_columns.object_id = tables.object_id
			INNER JOIN sys.schemas ON tables.schema_id = schemas.schema_id
			INNER JOIN sys.default_constraints ON all_columns.default_object_id = default_constraints.object_id
		WHERE schemas.name = 'dbo' AND tables.name = @TBL_NAME AND all_columns.name = @COL_NAME

	IF (@constraintname IS NOT NULL)
	BEGIN
		DECLARE @sql nvarchar(max) = 'ALTER TABLE [dbo].' + QUOTENAME(@TBL_NAME) + ' DROP CONSTRAINT ' + QUOTENAME(@constraintname)
		EXEC(@sql)
	END
END;

-- Similarly for primary key constraint
CREATE PROCEDURE #DROP_PRIMARY_KEY_CONSTRAINT @TBL_NAME sysname
AS
BEGIN
	DECLARE @constraintname sysname
	SELECT @constraintname = constraint_name
		FROM information_schema.table_constraints
		WHERE constraint_type = 'PRIMARY KEY' AND table_schema = 'dbo' AND table_name = @TBL_NAME
	IF @constraintname IS NOT NULL
	BEGIN
	    DECLARE @sql_pk nvarchar(max) = 'ALTER TABLE [dbo].' + QUOTENAME(@TBL_NAME) + ' DROP CONSTRAINT ' + @constraintname
	    EXEC(@sql_pk)
	end
END;

EXEC #DROP_DEFAULT_CONSTRAINT "PARTITION_PARAMS", "PARAM_VALUE";
ALTER TABLE "PARTITION_PARAMS" ALTER COLUMN "PARAM_VALUE" varchar(max);

-- HIVE-21077
ALTER TABLE DBS ADD CREATE_TIME INT;
ALTER TABLE CTLGS ADD CREATE_TIME INT;

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.2.0', VERSION_COMMENT='Hive release version 3.2.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.0 to 3.2.0' AS MESSAGE;

