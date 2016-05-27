-- Upgrade MetaStore schema from 2.0.0 to 2.1.0
RUN '034-HIVE-13076.derby.sql';
RUN '035-HIVE-13395.derby.sql';
RUN '036-HIVE-13354.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='2.1.0', VERSION_COMMENT='Hive release version 2.1.0' where VER_ID=1;
