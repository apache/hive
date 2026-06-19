SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.2.0' AS MESSAGE;

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

--:r 022-HIVE-14496.mssql.sql
ALTER TABLE TBLS ADD IS_REWRITE_ENABLED bit NOT NULL CONSTRAINT DEFAULT_IS_REWRITE_ENABLED DEFAULT(0);

--:r 023-HIVE-10562.mssql.sql
ALTER TABLE NOTIFICATION_LOG ADD MESSAGE_FORMAT nvarchar(16);

--:r 024-HIVE-12274.mssql.sql
EXEC #DROP_DEFAULT_CONSTRAINT "SERDE_PARAMS", "PARAM_VALUE";
ALTER TABLE "SERDE_PARAMS" ALTER COLUMN "PARAM_VALUE" nvarchar(MAX);
EXEC #DROP_DEFAULT_CONSTRAINT "TABLE_PARAMS", "PARAM_VALUE";
ALTER TABLE "TABLE_PARAMS" ALTER COLUMN "PARAM_VALUE" nvarchar(MAX);
EXEC #DROP_DEFAULT_CONSTRAINT "SD_PARAMS", "PARAM_VALUE";
ALTER TABLE "SD_PARAMS" ALTER COLUMN "PARAM_VALUE" nvarchar(MAX);

ALTER TABLE "TBLS" ALTER COLUMN "TBL_NAME" nvarchar(256);
ALTER TABLE "NOTIFICATION_LOG" ALTER COLUMN "TBL_NAME" nvarchar(256);
ALTER TABLE "PARTITION_EVENTS" ALTER COLUMN "TBL_NAME" nvarchar(256);
ALTER TABLE "TAB_COL_STATS" ALTER COLUMN "TABLE_NAME" nvarchar(256);
ALTER TABLE "PART_COL_STATS" ALTER COLUMN "TABLE_NAME" nvarchar(256);
ALTER TABLE "COMPLETED_TXN_COMPONENTS" ALTER COLUMN "CTC_TABLE" nvarchar(256);


-- A number of indices and constraints reference COLUMN_NAME.  These have to be dropped before the not null constraint
-- can be added. Earlier versions may not have created named constraints, so use IF EXISTS and also
-- the stored procedure.
ALTER TABLE COLUMNS_V2 DROP CONSTRAINT IF EXISTS COLUMNS_PK;
EXEC #DROP_PRIMARY_KEY_CONSTRAINT COLUMNS_V2;
DROP INDEX PARTITIONCOLUMNPRIVILEGEINDEX ON PART_COL_PRIVS;
DROP INDEX TABLECOLUMNPRIVILEGEINDEX ON TBL_COL_PRIVS;
DROP INDEX PCS_STATS_IDX ON PART_COL_STATS;

ALTER TABLE "COLUMNS_V2" ALTER COLUMN "COLUMN_NAME" nvarchar(767) NOT NULL;
ALTER TABLE "PART_COL_PRIVS" ALTER COLUMN "COLUMN_NAME" nvarchar(767) NULL;
ALTER TABLE "TBL_COL_PRIVS" ALTER COLUMN "COLUMN_NAME" nvarchar(767) NULL;
ALTER TABLE "SORT_COLS" ALTER COLUMN "COLUMN_NAME" nvarchar(767) NULL;
ALTER TABLE "TAB_COL_STATS" ALTER COLUMN "COLUMN_NAME" nvarchar(767) NOT NULL;
ALTER TABLE "PART_COL_STATS" ALTER COLUMN "COLUMN_NAME" nvarchar(767) NOT NULL;

-- Put back the indices and constraints we dropped.
ALTER TABLE COLUMNS_V2 ADD CONSTRAINT COLUMNS_PK PRIMARY KEY (CD_ID,"COLUMN_NAME");
CREATE INDEX PARTITIONCOLUMNPRIVILEGEINDEX ON PART_COL_PRIVS (PART_ID,"COLUMN_NAME",PRINCIPAL_NAME,PRINCIPAL_TYPE,PART_COL_PRIV,GRANTOR,GRANTOR_TYPE);
CREATE INDEX TABLECOLUMNPRIVILEGEINDEX ON TBL_COL_PRIVS (TBL_ID,"COLUMN_NAME",PRINCIPAL_NAME,PRINCIPAL_TYPE,TBL_COL_PRIV,GRANTOR,GRANTOR_TYPE);
CREATE INDEX PCS_STATS_IDX ON PART_COL_STATS (DB_NAME,TABLE_NAME,COLUMN_NAME,PARTITION_NAME);

UPDATE VERSION SET SCHEMA_VERSION='2.2.0', VERSION_COMMENT='Hive release version 2.2.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.2.0' AS MESSAGE;
