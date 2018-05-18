-- Upgrade MetaStore schema from 1.2.1000 to 2.0.0
RUN '021-HIVE-11970.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.0.0', VERSION_COMMENT='Hive release version 2.0.0' where VER_ID=1;
