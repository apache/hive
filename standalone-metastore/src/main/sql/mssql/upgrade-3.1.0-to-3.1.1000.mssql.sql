SELECT 'Upgrading MetaStore schema from 3.1.0 to 3.1.1000' AS MESSAGE;

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

EXEC #DROP_DEFAULT_CONSTRAINT "PARTITION_PARAMS", "PARAM_VALUE";
ALTER TABLE "PARTITION_PARAMS" ALTER COLUMN "PARAM_VALUE" varchar(max);

-- These lines need to be last.  Insert any changes above.
UPDATE VERSION SET SCHEMA_VERSION='3.1.1000', VERSION_COMMENT='Hive release version 3.1.1000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 3.1.0 to 3.1.1000' AS MESSAGE;
