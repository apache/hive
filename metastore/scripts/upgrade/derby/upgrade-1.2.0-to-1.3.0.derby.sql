-- Upgrade MetaStore schema from 1.2.0 to 1.3.0
RUN '021-HIVE-11970.derby.sql';
RUN '023-HIVE-12807.derby.sql';
RUN '024-HIVE-12814.derby.sql';
RUN '025-HIVE-12816.derby.sql';
RUN '026-HIVE-12818.derby.sql';
RUN '027-HIVE-12819.derby.sql';
RUN '028-HIVE-12821.derby.sql';
RUN '029-HIVE-12822.derby.sql';
RUN '030-HIVE-12823.derby.sql';
RUN '031-HIVE-12831.derby.sql';
RUN '032-HIVE-12832.derby.sql';
RUN '035-HIVE-13395.derby.sql';
RUN '036-HIVE-13354.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='1.3.0', VERSION_COMMENT='Hive release version 1.3.0' where VER_ID=1;
