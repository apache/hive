-- Upgrade MetaStore schema from 2.1.1000 to 2.1.2000

RUN '044-HIVE-16997.derby.sql';
RUN '045-HIVE-16886.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.1.2000', VERSION_COMMENT='Hive release version 2.1.2000' where VER_ID=1;
