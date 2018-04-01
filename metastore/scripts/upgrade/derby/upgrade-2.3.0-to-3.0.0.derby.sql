-- Upgrade MetaStore schema from 2.3.0 to 3.0.0
RUN '041-HIVE-16556.derby.sql';
RUN '042-HIVE-16575.derby.sql';
RUN '043-HIVE-16922.derby.sql';
RUN '044-HIVE-16997.derby.sql';
RUN '045-HIVE-16886.derby.sql';
RUN '046-HIVE-17566.derby.sql';
RUN '048-HIVE-14498.derby.sql';
RUN '049-HIVE-18489.derby.sql';
RUN '050-HIVE-18192.derby.sql';
RUN '051-HIVE-18675.derby.sql';
RUN '052-HIVE-18965.derby.sql';
RUN '053-HIVE-18755.derby.sql';
RUN '054-HIVE-18781.derby.sql';

UPDATE "APP".VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
