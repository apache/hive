package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;

/**
 * JethroData specific data accessor. This is needed because JethroData JDBC drivers do not support generic LIMIT and OFFSET
 * escape functions, and has  some special optimization for getting the query metadata using limit 0.
 */

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
    return "Select * from (" + sql + ") as \"tmp\" limit " + limit;
  }
  
  @Override
  protected String getMetaDataQuery(Configuration conf) {
    String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
    return addLimitToQuery(sql, 0);
  }
}
