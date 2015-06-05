-- Upgrade MetaStore schema from 1.2.0 to 1.3.0

UPDATE "APP".VERSION SET SCHEMA_VERSION='1.3.0', VERSION_COMMENT='Hive release version 1.3.0' where VER_ID=1;
