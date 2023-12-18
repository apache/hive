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
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.DID_NOT_INITIATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.FAILED_STATE;

public class CompactionCandidateHandler implements QueryHandler<Set<CompactionInfo>> {

  private final long checkInterval;
  private final int fetchSize;

  //language=SQL
  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return databaseProduct.addLimitClause(fetchSize, 
        "DISTINCT \"TC\".\"CTC_DATABASE\", \"TC\".\"CTC_TABLE\", \"TC\".\"CTC_PARTITION\" " +
        "FROM \"COMPLETED_TXN_COMPONENTS\" \"TC\" " + (checkInterval > 0 ?
        "LEFT JOIN ( " +
            "  SELECT \"C1\".* FROM \"COMPLETED_COMPACTIONS\" \"C1\" " +
            "  INNER JOIN ( " +
            "    SELECT MAX(\"CC_ID\") \"CC_ID\" FROM \"COMPLETED_COMPACTIONS\" " +
            "    GROUP BY \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\"" +
            "  ) \"C2\" " +
            "  ON \"C1\".\"CC_ID\" = \"C2\".\"CC_ID\" " +
            "  WHERE \"C1\".\"CC_STATE\" IN (:didNotInit, :failed)" +
            ") \"C\" " +
            "ON \"TC\".\"CTC_DATABASE\" = \"C\".\"CC_DATABASE\" AND \"TC\".\"CTC_TABLE\" = \"C\".\"CC_TABLE\" " +
            "  AND (\"TC\".\"CTC_PARTITION\" = \"C\".\"CC_PARTITION\" OR (\"TC\".\"CTC_PARTITION\" IS NULL AND \"C\".\"CC_PARTITION\" IS NULL)) " +
            "WHERE \"C\".\"CC_ID\" IS NOT NULL OR " + databaseProduct.isWithinCheckInterval("\"TC\".\"CTC_TIMESTAMP\"", checkInterval) : ""));
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("didNotInit", Character.toString(DID_NOT_INITIATE), Types.CHAR)
        .addValue("failed", Character.toString(FAILED_STATE), Types.CHAR);
  }  
  
  @Override
  public Set<CompactionInfo> extractData(ResultSet rs) throws SQLException, DataAccessException {
    Set<CompactionInfo> response = new HashSet<>();
    while (rs.next()) {
      CompactionInfo candidate = new CompactionInfo();
      candidate.dbname = rs.getString(1);
      candidate.tableName = rs.getString(2);
      candidate.partName = rs.getString(3);
      response.add(candidate);
    }
    return response;
  }

  public CompactionCandidateHandler(long lastChecked, int fetchSize) {
    checkInterval = (lastChecked <= 0) ? lastChecked : (System.currentTimeMillis() - lastChecked + 500) / 1000;
    this.fetchSize = fetchSize;
  }
}
