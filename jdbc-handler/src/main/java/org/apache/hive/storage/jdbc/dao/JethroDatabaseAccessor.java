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

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;

/**
 * JethroData specific data accessor. This is needed because JethroData JDBC drivers do
 * not support generic LIMIT and OFFSET escape functions, and has  some special optimization
 * for getting the query metadata using limit 0.
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
  protected String getMetaDataQuery(String sql) {
    return addLimitToQuery(sql, 0);
  }
}
