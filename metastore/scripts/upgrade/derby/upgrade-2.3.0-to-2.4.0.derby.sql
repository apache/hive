-- Upgrade MetaStore schema from 2.3.0 to 2.4.0

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.4.0', VERSION_COMMENT='Hive release version 2.4.0' where VER_ID=1;
