-- Upgrade MetaStore schema from 0.9.0 to 0.10.0
RUN '010-HIVE-3072.derby.sql';
RUN '011-HIVE-3649.derby.sql';
RUN '012-HIVE-1362.derby.sql';
