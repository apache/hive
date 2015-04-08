-- Upgrade MetaStore schema from 1.1.0 to 1.2.0

UPDATE "APP".VERSION SET SCHEMA_VERSION='1.2.0', VERSION_COMMENT='Hive release version 1.2.0' where VER_ID=1;
