-- Upgrade MetaStore schema from 0.5.0 to 0.6.0
RUN '001-HIVE-972.derby.sql';
RUN '002-HIVE-1068.derby.sql';
RUN '003-HIVE-675.derby.sql';
RUN '004-HIVE-1364.derby.sql';
