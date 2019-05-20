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
 * Oracle specific data accessor. This is needed because Oracle JDBC drivers do not support generic LIMIT and OFFSET
 * escape functions
 */
public class OracleDatabaseAccessor extends GenericJdbcDatabaseAccessor {

  // Random column name to reduce the chance of conflict
  static final String ROW_NUM_COLUMN_NAME = "dummy_rownum_col_rn1938392";

  @Override
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else {
      if (limit == -1) {
        return sql;
      }
      // A simple ROWNUM > offset and ROWNUM <= (offset + limit) won't work, it will return nothing
      return "SELECT * FROM (SELECT t.*, ROWNUM AS " + ROW_NUM_COLUMN_NAME + " FROM (" + sql + ") t) WHERE "
          +  ROW_NUM_COLUMN_NAME + " >" + offset + " AND " + ROW_NUM_COLUMN_NAME + " <=" + (offset + limit);
    }
  }


  @Override
  protected String addLimitToQuery(String sql, int limit) {
    if (limit == -1) {
      return sql;
    }
    return "SELECT * FROM (" + sql + ") WHERE ROWNUM <= " + limit;
  }

}
