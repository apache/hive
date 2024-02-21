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
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Returns the databases updated by txnId.
 * Queries TXN_TO_WRITE_ID using txnId.
 */
public class GetTxnDbsUpdatedHandler implements QueryHandler<List<String>> {
  
  private final long txnId;

  public GetTxnDbsUpdatedHandler(long txnId) {
    this.txnId = txnId;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "SELECT DISTINCT \"T2W_DATABASE\" FROM \"TXN_TO_WRITE_ID\" \"COMMITTED\" WHERE \"T2W_TXNID\" = :txnId";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource().addValue("txnId", txnId);
  }

  @Override
  public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
    List<String> dbsUpdated = new ArrayList<>();
    while (rs.next()) {
      dbsUpdated.add(rs.getString(1));
    }
    return dbsUpdated;
  }
}