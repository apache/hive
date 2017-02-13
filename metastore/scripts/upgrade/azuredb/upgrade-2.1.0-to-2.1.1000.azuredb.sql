SELECT 'Upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS MESSAGE;

/******** Update TYPE_NAME field to NVARCHAR(MAX)  ************/

DECLARE @constraint_name NVARCHAR(255);
SELECT @constraint_name = name from sys.default_constraints where parent_object_id = object_id(N'dbo.COLUMNS_V2')  AND col_name(parent_object_id, parent_column_id) = 'TYPE_NAME';
EXEC ('ALTER TABLE dbo.COLUMNS_V2 DROP CONSTRAINT ' + @constraint_name);
ALTER TABLE [dbo].[COLUMNS_V2] ALTER COLUMN [TYPE_NAME] NVARCHAR(MAX) NULL;
ALTER TABLE [dbo].[COLUMNS_V2] ADD DEFAULT (NULL) FOR [TYPE_NAME];

/****** Add in MESSAGE_FORMAT field to NOTIFICATION_LOG  ******/
IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'MESSAGE_FORMAT' and Object_ID = Object_ID(N'NOTIFICATION_LOG'))
BEGIN
ALTER TABLE [dbo].[NOTIFICATION_LOG] ADD [MESSAGE_FORMAT] [nvarchar](16) NULL;
END


UPDATE [dbo].[VERSION] SET SCHEMA_VERSION='2.1.1000', VERSION_COMMENT='Hive release version 2.1.1000' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.1.0 to 2.1.1000' AS MESSAGE;
