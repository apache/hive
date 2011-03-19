-- Upgrade MetaStore schema from 0.6.0 to 0.7.0
RUN '005-HIVE-417.derby.sql';
RUN '006-HIVE-1823.derby.sql';
RUN '007-HIVE-78.derby.sql';
