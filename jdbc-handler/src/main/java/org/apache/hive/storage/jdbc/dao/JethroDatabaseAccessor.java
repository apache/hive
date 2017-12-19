package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;

public class JethroDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      return sql + " LIMIT " + offset + "," + limit;
    }
  }

  @Override
  protected String addLimitToQuery(String sql, int limit) {
    return sql + " LIMIT " + limit;
  }
  
  @Override
  protected String getMetaDataQuery(Configuration conf) {
    String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
    return addLimitToQuery(sql, 0);
  }
}
