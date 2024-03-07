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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.RemoveTxnsFromMinHistoryLevelCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.RemoveWriteIdsFromMinHistoryCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetDatabaseIdHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.UncategorizedSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.executeQueriesInBatch;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.executeQueriesInBatchNoCount;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class AbortTxnsFunction implements TransactionalFunction<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(AbortTxnsFunction.class);

  private final List<Long> txnids;
  private final boolean checkHeartbeat;
  private final boolean skipCount;
  private final boolean isReplReplayed;
  private final TxnErrorMsg txnErrorMsg;

  /**
   * TODO: expose this as an operation to client.  Useful for streaming API to abort all remaining
   * transactions in a batch on IOExceptions.
   * Caller must rollback the transaction if not all transactions were aborted since this will not
   * attempt to delete associated locks in this case.
   *
   * @param txnids list of transactions to abort
   * @param checkHeartbeat value used by {@code  org.apache.hadoop.hive.metastore.txn.TxnHandler#performTimeOuts()} 
   *                       to ensure this doesn't Abort txn which were heartbeated after #performTimeOuts() select 
   *                       and this operation.
   * @param skipCount If true, the method always returns 0, otherwise returns the number of actually aborted txns
   */
  public AbortTxnsFunction(List<Long> txnids, boolean checkHeartbeat, boolean skipCount, boolean isReplReplayed, 
                           TxnErrorMsg txnErrorMsg) {
    this.txnids = txnids;
    this.checkHeartbeat = checkHeartbeat;
    this.skipCount = skipCount;
    this.isReplReplayed = isReplReplayed;
    this.txnErrorMsg = txnErrorMsg;
  }

  /**
   * @param jdbcResource A {@link MultiDataSourceJdbcResource} instance responsible for providing all the necessary 
   *                     resources to be able to perform transactional database calls.
   * @return 0 if skipCount is true, the number of aborted transactions otherwise
   */
  @Override
  public Integer execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    if (txnids.isEmpty()) {
      return 0;
    }
    Configuration conf = jdbcResource.getConf();
    Collections.sort(txnids);    
    LOG.debug("Aborting {} transaction(s) {} due to {}", txnids.size(), txnids, txnErrorMsg);
    
    int maxBatchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.JDBC_MAX_BATCH_SIZE);    
    jdbcResource.execute(new RemoveTxnsFromMinHistoryLevelCommand(txnids));
    jdbcResource.execute(new RemoveWriteIdsFromMinHistoryCommand(txnids));
    
    Connection dbConn = jdbcResource.getConnection();
    try {
      DatabaseProduct dbProduct = jdbcResource.getDatabaseProduct();
      //This is an update statement, thus at any Isolation level will take Write locks so will block
      //all other ops using S4U on TXNS row.
      List<String> queries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      // add update txns queries to query list
      prefix.append("UPDATE \"TXNS\" SET \"TXN_STATE\" = ").append(TxnStatus.ABORTED)
          .append(" , \"TXN_META_INFO\" = ").append(txnErrorMsg.toSqlString())
          .append(" WHERE \"TXN_STATE\" = ").append(TxnStatus.OPEN).append(" AND ");
      if (checkHeartbeat) {
        suffix.append(" AND \"TXN_LAST_HEARTBEAT\" < ")
            .append(getEpochFn(dbProduct)).append("-")
            .append(MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS));
      }
      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "\"TXN_ID\"", true, false);
      int numUpdateQueries = queries.size();

      // add delete hive locks queries to query list
      prefix.setLength(0);
      suffix.setLength(0);
      prefix.append("DELETE FROM \"HIVE_LOCKS\" WHERE ");
      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "\"HL_TXNID\"", false, false);

      //If this abort is for REPL_CREATED TXN initiated outside the replication flow, then clean the corresponding entry
      //from REPL_TXN_MAP and mark that database as replication incompatible.
      if (!isReplReplayed) {
        for (String database : getDbNamesForReplayedTxns(jdbcResource, dbConn, txnids)) {
          markDbAsReplIncompatible(jdbcResource, database);
        }
        // Delete mapping from REPL_TXN_MAP if it exists.
        prefix.setLength(0);
        suffix.setLength(0);
        prefix.append("DELETE FROM \"REPL_TXN_MAP\" WHERE ");
        TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "\"RTM_TARGET_TXN_ID\"", false, false);
      }

      int numAborted = 0;
      try (Statement stmt = dbConn.createStatement()) {
        // execute all queries in the list in one batch
        if (skipCount) {
          executeQueriesInBatchNoCount(dbProduct, stmt, queries, maxBatchSize);
        } else {
          List<Integer> affectedRowsByQuery = executeQueriesInBatch(stmt, queries, maxBatchSize);
          numAborted = getUpdateCount(numUpdateQueries, affectedRowsByQuery);
        }
      }

      if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
        Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_ABORTED_TXNS).inc(txnids.size());
      }
      LOG.warn("Aborted {} transaction(s) {} due to {}", txnids.size(), txnids, txnErrorMsg);
      return numAborted;
    } catch (SQLException e) {
      throw new UncategorizedSQLException(null, null, e);
    }
  }

  private Set<String> getDbNamesForReplayedTxns(MultiDataSourceJdbcResource jdbcResource, Connection dbConn, 
                                                List<Long> targetTxnIds) throws SQLException {
    Set<String> dbNames = new HashSet<>();
    if (targetTxnIds.isEmpty()) {
      return dbNames;
    }
    List<String> inQueries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    prefix.append("SELECT \"RTM_REPL_POLICY\" FROM \"REPL_TXN_MAP\" WHERE ");
    TxnUtils.buildQueryWithINClause(jdbcResource.getConf(), inQueries, prefix, new StringBuilder(), targetTxnIds,
        "\"RTM_TARGET_TXN_ID\"", false, false);
    for (String query : inQueries) {
      LOG.debug("Going to execute select <{}>", query);
      try (PreparedStatement pst = jdbcResource.getSqlGenerator().prepareStmtWithParameters(dbConn, query, null);
           ResultSet rs = pst.executeQuery()) {
        while (rs.next()) {
          dbNames.add(MetaStoreUtils.getDbNameFromReplPolicy(rs.getString(1)));
        }
      }
    }
    return dbNames;
  }

  private void markDbAsReplIncompatible(MultiDataSourceJdbcResource jdbcResource, String database) throws SQLException, MetaException {
    try (Statement stmt = jdbcResource.getConnection().createStatement()){
      String catalog = MetaStoreUtils.getDefaultCatalog(jdbcResource.getConf());
      String s = jdbcResource.getSqlGenerator().getDbProduct().getPrepareTxnStmt();
      if (s != null) {
        stmt.execute(s);
      }
      long dbId = jdbcResource.execute(new GetDatabaseIdHandler(database, catalog));
      new UpdataDatabasePropFunction(database, dbId, ReplConst.REPL_INCOMPATIBLE, ReplConst.TRUE).execute(jdbcResource);
    }
  }

  private int getUpdateCount(int numUpdateQueries, List<Integer> affectedRowsByQuery) {
    return affectedRowsByQuery.stream()
        .limit(numUpdateQueries)
        .mapToInt(Integer::intValue)
        .sum();
  }


}
