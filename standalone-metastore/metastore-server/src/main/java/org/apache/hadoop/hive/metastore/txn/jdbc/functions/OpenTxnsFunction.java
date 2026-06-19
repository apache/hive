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

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.TargetTxnIdListHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class OpenTxnsFunction implements TransactionalFunction<List<Long>> {

  private static final Logger LOG = LoggerFactory.getLogger(OpenTxnsFunction.class);

  private static final String TXN_TMP_STATE = "_";
  private static final String TXNS_INSERT_QRY = "INSERT INTO \"TXNS\" " +
      "(\"TXN_STATE\", \"TXN_STARTED\", \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\", \"TXN_TYPE\") " +
      "VALUES(?,%s,%s,?,?,?)";

  private final OpenTxnRequest rqst;
  private final long openTxnTimeOutMillis;
  private final List<TransactionalMetaStoreEventListener> transactionalListeners;

  public OpenTxnsFunction(OpenTxnRequest rqst, long openTxnTimeOutMillis, 
                          List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.rqst = rqst;
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public List<Long> execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    DatabaseProduct dbProduct = jdbcResource.getDatabaseProduct();
    int numTxns = rqst.getNum_txns();
    // Make sure the user has not requested an insane amount of txns.
    int maxTxns = MetastoreConf.getIntVar(jdbcResource.getConf(), MetastoreConf.ConfVars.TXN_MAX_OPEN_BATCH);
    if (numTxns > maxTxns) {
      numTxns = maxTxns;
    }
    List<PreparedStatement> insertPreparedStmts = null;
    TxnType txnType = rqst.isSetTxn_type() ? rqst.getTxn_type() : TxnType.DEFAULT;
    boolean isReplayedReplTxn = txnType == TxnType.REPL_CREATED;
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && txnType == TxnType.DEFAULT;
    if (isReplayedReplTxn) {
      assert rqst.isSetReplPolicy();
      List<Long> targetTxnIdList = jdbcResource.execute(new TargetTxnIdListHandler(rqst.getReplPolicy(), rqst.getReplSrcTxnIds()));

      if (!targetTxnIdList.isEmpty()) {
        if (targetTxnIdList.size() != rqst.getReplSrcTxnIds().size()) {
          LOG.warn("target txn id number {} is not matching with source txn id number {}",
              targetTxnIdList, rqst.getReplSrcTxnIds());
        }
        LOG.info("Target transactions {} are present for repl policy : {} and Source transaction id : {}",
            targetTxnIdList, rqst.getReplPolicy(), rqst.getReplSrcTxnIds().toString());
        return targetTxnIdList;
      }
    }

    long minOpenTxnId = 0;
    if (TxnHandler.ConfVars.useMinHistoryLevel()) {
      minOpenTxnId = new MinOpenTxnIdWaterMarkFunction(openTxnTimeOutMillis).execute(jdbcResource);
    }

    List<Long> txnIds = new ArrayList<>(numTxns);
    /*
     * The getGeneratedKeys are not supported in every dbms, after executing a multi line insert.
     * But it is supported in every used dbms for single line insert, even if the metadata says otherwise.
     * If the getGeneratedKeys are not supported first we insert a random batchId in the TXN_META_INFO field,
     * then the keys are selected beck with that batchid.
     */
    boolean genKeySupport = dbProduct.supportsGetGeneratedKeys();
    genKeySupport = genKeySupport || (numTxns == 1);

    String insertQuery = String.format(TXNS_INSERT_QRY, getEpochFn(dbProduct), getEpochFn(dbProduct));
    LOG.debug("Going to execute insert <{}>", insertQuery);

    Connection dbConn = jdbcResource.getConnection();
    NamedParameterJdbcTemplate namedParameterJdbcTemplate = jdbcResource.getJdbcTemplate();
    int maxBatchSize = MetastoreConf.getIntVar(jdbcResource.getConf(), MetastoreConf.ConfVars.JDBC_MAX_BATCH_SIZE);
    try (PreparedStatement ps = dbConn.prepareStatement(insertQuery, new String[]{ "TXN_ID" })) {
      String state = genKeySupport ? TxnStatus.OPEN.getSqlConst() : TXN_TMP_STATE;
      if (numTxns == 1) {
        ps.setString(1, state);
        ps.setString(2, rqst.getUser());
        ps.setString(3, rqst.getHostname());
        ps.setInt(4, txnType.getValue());
        txnIds.addAll(executeTxnInsertBatchAndExtractGeneratedKeys(namedParameterJdbcTemplate, true, ps, false));
      } else {
        for (int i = 0; i < numTxns; ++i) {
          ps.setString(1, state);
          ps.setString(2, rqst.getUser());
          ps.setString(3, rqst.getHostname());
          ps.setInt(4, txnType.getValue());
          ps.addBatch();

          if ((i + 1) % maxBatchSize == 0) {
            txnIds.addAll(executeTxnInsertBatchAndExtractGeneratedKeys(namedParameterJdbcTemplate, genKeySupport, ps, true));
          }
        }
        if (numTxns % maxBatchSize != 0) {
          txnIds.addAll(executeTxnInsertBatchAndExtractGeneratedKeys(namedParameterJdbcTemplate, genKeySupport, ps, true));
        }
      }
    } catch (SQLException e) {
      throw new UncategorizedSQLException(null, null, e);
    }

    assert txnIds.size() == numTxns;

    addTxnToMinHistoryLevel(jdbcResource.getJdbcTemplate().getJdbcTemplate(), maxBatchSize, txnIds, minOpenTxnId);

    if (isReplayedReplTxn) {
      List<String> rowsRepl = new ArrayList<>(numTxns);
      List<String> params = Collections.singletonList(rqst.getReplPolicy());
      List<List<String>> paramsList = new ArrayList<>(numTxns);
      for (int i = 0; i < numTxns; i++) {
        rowsRepl.add("?," + rqst.getReplSrcTxnIds().get(i) + "," + txnIds.get(i));
        paramsList.add(params);
      }

      try {
        insertPreparedStmts = jdbcResource.getSqlGenerator().createInsertValuesPreparedStmt(dbConn,
            "\"REPL_TXN_MAP\" (\"RTM_REPL_POLICY\", \"RTM_SRC_TXN_ID\", \"RTM_TARGET_TXN_ID\")", rowsRepl,
            paramsList);
        for (PreparedStatement pst : insertPreparedStmts) {
          try (PreparedStatement ppst = pst) {
            ppst.execute();
          }
        }
      } catch (SQLException e) {
        throw new UncategorizedSQLException(null, null, e);
      }
    }

    if (transactionalListeners != null && !isHiveReplTxn) {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
          EventMessage.EventType.OPEN_TXN, new OpenTxnEvent(txnIds, txnType), dbConn, jdbcResource.getSqlGenerator());
    }
    return txnIds;
  }

  /**
   * Add min history level entry for each generated txn record
   * @param jdbcTemplate {@link NamedParameterJdbcTemplate} to use for command execution
   * @param txnIds new transaction ids
   * @deprecated Remove this method when min_history_level table is dropped
   * @throws SQLException ex
   */
  @Deprecated
  private void addTxnToMinHistoryLevel(JdbcTemplate jdbcTemplate, int batchSize, List<Long> txnIds, long minOpenTxnId) {
    if (!TxnHandler.ConfVars.useMinHistoryLevel()) {
      return;
    }
    String sql = "INSERT INTO \"MIN_HISTORY_LEVEL\" (\"MHL_TXNID\", \"MHL_MIN_OPEN_TXNID\") VALUES(?, ?)";
    LOG.debug("Going to execute insert batch: <{}>", sql);

    jdbcTemplate.batchUpdate(sql, txnIds, batchSize, (ps, argument) -> {
      ps.setLong(1, argument);
      ps.setLong(2, minOpenTxnId);
    });
    
    LOG.info("Added entries to MIN_HISTORY_LEVEL for current txns: ({}) with min_open_txn: {}", txnIds, minOpenTxnId);
  }

  private List<Long> executeTxnInsertBatchAndExtractGeneratedKeys(NamedParameterJdbcTemplate jdbcTemplate, boolean genKeySupport,
                                                                  PreparedStatement ps, boolean batch) throws SQLException {
    
    List<Long> txnIds = new ArrayList<>();
    if (batch) {
      ps.executeBatch();
    } else {
      // For slight performance advantage we do not use the executeBatch, when we only have one row
      ps.execute();
    }
    if (genKeySupport) {
      try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
        while (generatedKeys.next()) {
          txnIds.add(generatedKeys.getLong(1));
        }
      }
    } else {
      txnIds = jdbcTemplate.query("SELECT \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = :tmpState",
          new MapSqlParameterSource().addValue("tmpState", TXN_TMP_STATE), (rs, rowNum) -> rs.getLong(1));

      jdbcTemplate.update("UPDATE \"TXNS\" SET \"TXN_STATE\" = :newState WHERE \"TXN_STATE\" = :tmpState",
          new MapSqlParameterSource()
              .addValue("newState", TxnStatus.OPEN.getSqlConst())
              .addValue("tmpState", TXN_TMP_STATE));
    }
    return txnIds;
  }

}
