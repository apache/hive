-- Upgrade MetaStore schema from 1.3.0 to 2.0.0

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.0.0', VERSION_COMMENT='Hive release version 2.0.0' where VER_ID=1;
