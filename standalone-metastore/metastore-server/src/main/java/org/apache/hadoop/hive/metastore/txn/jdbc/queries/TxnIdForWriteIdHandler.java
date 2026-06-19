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

public class TxnIdForWriteIdHandler implements QueryHandler<Long> {
  
  private final long writeId;
  private final String dbName;
  private final String tableName;

  public TxnIdForWriteIdHandler(long writeId, String dbName, String tableName) {
    this.writeId = writeId;
    this.dbName = dbName;
    this.tableName = tableName;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "SELECT \"T2W_TXNID\" FROM \"TXN_TO_WRITE_ID\" WHERE"
        + " \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? AND \"T2W_WRITEID\" = " + writeId;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("writeId", writeId)
        .addValue("dbName", dbName)
        .addValue("tableName", tableName);
  }

  @Override
  public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
    long txnId = -1;
    if (rs.next()) {
      txnId = rs.getLong(1);
    }
    return txnId;
  }
}
