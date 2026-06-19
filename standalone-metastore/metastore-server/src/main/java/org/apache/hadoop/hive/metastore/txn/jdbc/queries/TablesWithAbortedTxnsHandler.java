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
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;
import java.util.TreeSet;

public class TablesWithAbortedTxnsHandler implements QueryHandler<Set<String>> {

  //language=SQL
  private static final String SELECT_TABLES_WITH_X_ABORTED_TXNS =
      "SELECT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" FROM \"TXN_COMPONENTS\" " +
          "INNER JOIN \"TXNS\" ON \"TC_TXNID\" = \"TXN_ID\" WHERE \"TXN_STATE\" = :abortedState " +
          "GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" HAVING COUNT(\"TXN_ID\") > :txnThreshold";
  
  private final int txnThreshold;

  public TablesWithAbortedTxnsHandler(int txnThreshold) {
    this.txnThreshold = txnThreshold;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return SELECT_TABLES_WITH_X_ABORTED_TXNS;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("abortedState", TxnStatus.ABORTED.getSqlConst(), Types.CHAR)
        .addValue("txnThreshold", txnThreshold);
  }

  @Override
  public Set<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
    Set<String> resourceNames = new TreeSet<>();
    while (rs.next()) {
      String resourceName = rs.getString(1) + "." + rs.getString(2);
      String partName = rs.getString(3);
      resourceName = partName != null ? resourceName + "#" + partName : resourceName;
      resourceNames.add(resourceName);
    }
    return resourceNames;
  }
}
