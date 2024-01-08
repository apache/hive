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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class GetWriteIdsHandler implements QueryHandler<Map<Pair<String, String>, Long>> {


  //language=SQL
  private static final String SELECT_WRITE_ID_QUERY = "SELECT \"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\" " +
      "FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_TXNID\" = :txnId ";

  private final LockRequest lockRequest;

  public GetWriteIdsHandler(LockRequest lockRequest) {
    this.lockRequest = lockRequest;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    StringBuilder sb = new StringBuilder(SELECT_WRITE_ID_QUERY);
    sb.append(" AND (");
    for(int i = 0; i< lockRequest.getComponentSize(); i++) {
      sb.append("(\"T2W_DATABASE\" = ").append(":db").append(i)
          .append(" AND \"T2W_TABLE\" = :table").append(i).append(")");
      if(i < lockRequest.getComponentSize() - 1) {
        sb.append(" OR ");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("txnId", lockRequest.getTxnid());
    for(int i = 0; i< lockRequest.getComponentSize(); i++) {
      params.addValue("db" + i, lockRequest.getComponent().get(i).getDbname());
      params.addValue("table" + i, lockRequest.getComponent().get(i).getTablename());
    }
    return params;
  }

  @Override
  public Map<Pair<String, String>, Long> extractData(ResultSet rs) throws SQLException, DataAccessException {
    Map<Pair<String, String>, Long> writeIds = new HashMap<>();
    while (rs.next()) {
      writeIds.put(Pair.of(rs.getString("T2W_DATABASE"), rs.getString("T2W_TABLE")), rs.getLong("T2W_WRITEID"));
    }
    return writeIds;
  }
}
