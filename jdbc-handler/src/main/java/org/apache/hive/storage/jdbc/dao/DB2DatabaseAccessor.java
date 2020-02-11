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

/**
 * DB2 specific data accessor. DB2 JDBC drivers works similar to Postgres, so the current
 * implementation of DB2DatabaseAccessor is the same as PostgresDatabaseAccessor
 */
public class DB2DatabaseAccessor extends GenericJdbcDatabaseAccessor {
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
  protected String constructQuery(String table, String[] columnNames) {
    if(columnNames == null) {
      throw new IllegalArgumentException("Column names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table).append(" VALUES (");

    for (int i = 0; i < columnNames.length; i++) {
      query.append("?");
      if(i != columnNames.length - 1) {
        query.append(",");
      }
    }
    query.append(")");
    return query.toString();
  }
}
