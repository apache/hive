-- Upgrade MetaStore schema from 2.2.0 to 3.0.0

UPDATE "APP".VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
