-- Upgrade MetaStore schema from 0.11.0 to 0.12.0
RUN '013-HIVE-3255.derby.sql';
RUN '014-HIVE-3764.derby.sql';
UPDATE "APP".VERSION SET SCHEMA_VERSION='0.12.0', VERSION_COMMENT='Hive release version 0.12.0' where VER_ID=1;
