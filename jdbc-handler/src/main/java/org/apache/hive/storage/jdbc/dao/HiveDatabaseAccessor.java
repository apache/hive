/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.dao;

import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

/**
 * Hive specific data accessor.
 */
public class HiveDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  @Override
  public List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
    List<String> columnNames = super.getColumnNames(conf);
    return columnNames.stream()
        .map(c -> {
          int lastIndex = c.lastIndexOf(".");
          return lastIndex == -1 ? c : c.substring(lastIndex + 1); })
        .collect(Collectors.toList());
  }

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      if (limit == -1) {
        return sql;
      }
      return sql + " LIMIT " + limit + " OFFSET " + offset;
    }
  }

  @Override
  protected String addLimitToQuery(String sql, int limit) {
    if (limit == -1) {
      return sql;
    }
    return sql + " LIMIT " + limit;
  }

  @Override
  protected List<String> getColNamesFromRS(ResultSet rs) throws Exception {
    List<String> columnNames = super.getColNamesFromRS(rs);
    return columnNames.stream()
        .map(c -> {
          int lastIndex = c.lastIndexOf(".");
          return lastIndex == -1 ? c : c.substring(lastIndex + 1); })
        .collect(Collectors.toList());
  }

  @Override
  protected String getMetaDataQuery(String sql) {
    return addLimitToQuery(sql, 0);
  }
}
