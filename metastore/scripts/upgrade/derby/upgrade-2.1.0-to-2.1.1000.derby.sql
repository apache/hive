-- Upgrade MetaStore schema from 2.1.0 to 2.1.1000

RUN '038-HIVE-10562.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.1.1000', VERSION_COMMENT='Hive release version 2.1.1000' where VER_ID=1;
