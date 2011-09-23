-- Upgrade MetaStore schema from 0.7.0 to 0.8.0
RUN '008-HIVE-2246.derby.sql';
RUN '009-HIVE-2215.derby.sql';
