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
package org.apache.hadoop.hive.metastore.txn.jdbc.queries;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.apache.hadoop.hive.metastore.txn.entities.LockInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * NEVER call this function without first calling heartbeat(long, long)
 */
public class GetLocksByLockId implements QueryHandler<List<LockInfo>> {

  private static final String noSelectQuery = " \"HL_LOCK_EXT_ID\", \"HL_LOCK_INT_ID\", " +
      "\"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\", \"HL_LOCK_STATE\", \"HL_LOCK_TYPE\", \"HL_TXNID\" " +
      "FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = :extLockId";
  
  private final long extLockId;
  private final int limit;
  private final SQLGenerator sqlGenerator;

  public GetLocksByLockId(long extLockId, int limit, SQLGenerator sqlGenerator) {
    this.extLockId = extLockId;
    this.limit = limit;
    this.sqlGenerator = sqlGenerator;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    if (limit > 0) {
      return sqlGenerator.addLimitClause(limit, noSelectQuery);
    } else {
      return "SELECT " + noSelectQuery;      
    }
 }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource().addValue("extLockId", extLockId);
  }

  @Override
  public List<LockInfo> extractData(ResultSet rs) throws SQLException, DataAccessException {
    List<LockInfo> result = new ArrayList<>();
    while (rs.next()) {
      try {
        result.add(new LockInfo(rs));
      } catch (MetaException e) {
        throw new MetaWrapperException(e);
      }
    }
    return result;
  }
  
}
