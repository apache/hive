-- Upgrade MetaStore schema from 0.13.0 to 0.14.0

UPDATE "APP".VERSION SET SCHEMA_VERSION='0.14.0', VERSION_COMMENT='Hive release version 0.14.0' where VER_ID=1;
