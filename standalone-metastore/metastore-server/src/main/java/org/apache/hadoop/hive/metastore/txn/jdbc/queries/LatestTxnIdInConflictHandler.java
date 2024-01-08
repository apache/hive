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

import com.sun.tools.javac.util.List;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.OperationType;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class LatestTxnIdInConflictHandler implements QueryHandler<Long> {
  
  private final long txnId;

  public LatestTxnIdInConflictHandler(long txnId) {
    this.txnId = txnId;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return 
        " SELECT MAX(\"COMMITTED\".\"WS_TXNID\")" +
        " FROM \"WRITE_SET\" \"COMMITTED\"" +
        " INNER JOIN (" +
        "   SELECT DISTINCT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_TXNID\"" +
        "   FROM \"TXN_COMPONENTS\"" +
        "   WHERE \"TC_TXNID\" = :txnId" +
        "     AND \"TC_OPERATION_TYPE\" IN (:types)" +
        " ) \"CUR\"" +
        " ON \"COMMITTED\".\"WS_DATABASE\" = \"CUR\".\"TC_DATABASE\"" +
        "   AND \"COMMITTED\".\"WS_TABLE\" = \"CUR\".\"TC_TABLE\"" +
        (TxnHandler.ConfVars.useMinHistoryLevel() ? "" :
        "   AND \"COMMITTED\".\"WS_OPERATION_TYPE\" != :wsType") + 
        // For partitioned table we always track writes at partition level (never at table)
        // and for non partitioned - always at table level, thus the same table should never
        // have entries with partition key and w/o
        "   AND (\"COMMITTED\".\"WS_PARTITION\" = \"CUR\".\"TC_PARTITION\" OR" +
        "     \"CUR\".\"TC_PARTITION\" IS NULL) " +
        // txns overlap
        " WHERE \"CUR\".\"TC_TXNID\" <= \"COMMITTED\".\"WS_COMMIT_ID\"";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("txnId", txnId)
        .addValue("types", List.of(OperationType.UPDATE.getSqlConst(), OperationType.DELETE.getSqlConst()), Types.CHAR)
        .addValue("wsType", OperationType.INSERT.getSqlConst(), Types.CHAR);        
  }

  @Override
  public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
    return rs.next() ? rs.getLong(1) : -1;
  }
  
}
