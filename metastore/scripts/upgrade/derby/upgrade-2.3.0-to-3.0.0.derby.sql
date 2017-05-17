-- Upgrade MetaStore schema from 2.3.0 to 3.0.0

RUN '041-HIVE-16556.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
