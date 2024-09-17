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
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.txn.entities.MetricsInfo;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.READY_FOR_CLEANING;

public class MetricsInfoHandler implements QueryHandler<MetricsInfo> {
  
  public static final MetricsInfoHandler INSTANCE = new MetricsInfoHandler();
  
  //language=SQL
  private static final String SELECT_METRICS_INFO_QUERY =
      "SELECT * FROM (SELECT COUNT(*) FROM \"TXN_TO_WRITE_ID\") \"TTWID\" CROSS JOIN (" +
          "SELECT COUNT(*) FROM \"COMPLETED_TXN_COMPONENTS\") \"CTC\" CROSS JOIN (" +
          "SELECT COUNT(*), MIN(\"TXN_ID\"), ({0} - MIN(\"TXN_STARTED\"))/1000 FROM \"TXNS\" " +
          "   WHERE \"TXN_STATE\"= :openStatus AND \"TXN_TYPE\" = :replCreatedType) \"TR\" CROSS JOIN (" +
          "SELECT COUNT(*), MIN(\"TXN_ID\"), ({0} - MIN(\"TXN_STARTED\"))/1000 FROM \"TXNS\" " +
          "   WHERE \"TXN_STATE\"= :openStatus AND \"TXN_TYPE\" != :replCreatedType) \"T\" CROSS JOIN (" +
          "SELECT COUNT(*), MIN(\"TXN_ID\"), ({0} - MIN(\"TXN_STARTED\"))/1000 FROM \"TXNS\" " +
          "   WHERE \"TXN_STATE\"= :abortedStatus) \"A\" CROSS JOIN (" +
          "SELECT COUNT(*), ({0} - MIN(\"HL_ACQUIRED_AT\"))/1000 FROM \"HIVE_LOCKS\") \"HL\" CROSS JOIN (" +
          "SELECT ({0} - MIN(\"CQ_COMMIT_TIME\"))/1000 from \"COMPACTION_QUEUE\" " +
          "   WHERE \"CQ_STATE\"= :readyForCleaningState) OLDEST_CLEAN";

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return MessageFormat.format(SELECT_METRICS_INFO_QUERY, TxnUtils.getEpochFn(databaseProduct));
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("openStatus", TxnStatus.OPEN.getSqlConst(), Types.CHAR)
        .addValue("abortedStatus", TxnStatus.ABORTED.getSqlConst(), Types.CHAR)
        .addValue("replCreatedType", TxnType.REPL_CREATED.getValue())
        .addValue("readyForCleaningState", Character.toString(READY_FOR_CLEANING), Types.CHAR);
  }

  @Override
  public MetricsInfo extractData(ResultSet rs) throws SQLException, DataAccessException {
    MetricsInfo metrics = new MetricsInfo();
    if (rs.next()) {
      metrics.setTxnToWriteIdCount(rs.getInt(1));
      metrics.setCompletedTxnsCount(rs.getInt(2));
      metrics.setOpenReplTxnsCount(rs.getInt(3));
      metrics.setOldestOpenReplTxnId(rs.getInt(4));
      metrics.setOldestOpenReplTxnAge(rs.getInt(5));
      metrics.setOpenNonReplTxnsCount(rs.getInt(6));
      metrics.setOldestOpenNonReplTxnId(rs.getInt(7));
      metrics.setOldestOpenNonReplTxnAge(rs.getInt(8));
      metrics.setAbortedTxnsCount(rs.getInt(9));
      metrics.setOldestAbortedTxnId(rs.getInt(10));
      metrics.setOldestAbortedTxnAge(rs.getInt(11));
      metrics.setLocksCount(rs.getInt(12));
      metrics.setOldestLockAge(rs.getInt(13));
      metrics.setOldestReadyForCleaningAge(rs.getInt(14));
    }
    return metrics;
  }
  
  
}
