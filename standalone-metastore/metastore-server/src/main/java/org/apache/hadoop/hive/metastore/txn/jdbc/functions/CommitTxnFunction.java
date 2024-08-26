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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.ReplLastIdInfo;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.entities.OperationType;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.DeleteReplTxnMapEntryCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.InsertCompletedTxnComponentsCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.RemoveTxnsFromMinHistoryLevelCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.RemoveWriteIdsFromMinHistoryCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.FindTxnStateHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetCompactionInfoHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetHighWaterMarkHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetOpenTxnTypeAndLockHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.TargetTxnIdListHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.RollbackException;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class CommitTxnFunction implements TransactionalFunction<TxnType> {

  private static final Logger LOG = LoggerFactory.getLogger(CommitTxnFunction.class);

  private final CommitTxnRequest rqst;
  private final List<TransactionalMetaStoreEventListener> transactionalListeners;

  public CommitTxnFunction(CommitTxnRequest rqst, List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.rqst = rqst;
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public TxnType execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, NoSuchTxnException, TxnAbortedException {
    char isUpdateDelete = 'N';
    long txnid = rqst.getTxnid();
    long sourceTxnId = -1;

    boolean isReplayedReplTxn = TxnType.REPL_CREATED.equals(rqst.getTxn_type());
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    // Get the current TXN
    TransactionContext context = jdbcResource.getTransactionManager().getActiveTransaction();
    Long commitId = null;

    if (rqst.isSetReplLastIdInfo()) {
      updateReplId(jdbcResource, rqst.getReplLastIdInfo());
    }

    if (isReplayedReplTxn) {
      assert (rqst.isSetReplPolicy());
      sourceTxnId = rqst.getTxnid();
      List<Long> targetTxnIds = jdbcResource.execute(new TargetTxnIdListHandler(rqst.getReplPolicy(), Collections.singletonList(sourceTxnId)));
      if (targetTxnIds.isEmpty()) {
        // Idempotent case where txn was already closed or commit txn event received without
        // corresponding open txn event.
        LOG.info("Target txn id is missing for source txn id : {} and repl policy {}", sourceTxnId,
            rqst.getReplPolicy());
        throw new RollbackException(null);
      }
      assert targetTxnIds.size() == 1;
      txnid = targetTxnIds.get(0);
    }

    /**
     * Runs at READ_COMMITTED with S4U on TXNS row for "txnid".  S4U ensures that no other
     * operation can change this txn (such acquiring locks). While lock() and commitTxn()
     * should not normally run concurrently (for same txn) but could due to bugs in the client
     * which could then corrupt internal transaction manager state.  Also competes with abortTxn().
     */
    TxnType txnType = jdbcResource.execute(new GetOpenTxnTypeAndLockHandler(jdbcResource.getSqlGenerator(), txnid));
    if (txnType == null) {
      //if here, txn was not found (in expected state)
      TxnStatus actualTxnStatus = jdbcResource.execute(new FindTxnStateHandler(txnid));
      if (actualTxnStatus == TxnStatus.COMMITTED) {
        if (isReplayedReplTxn) {
          // in case of replication, idempotent is taken care by getTargetTxnId
          LOG.warn("Invalid state COMMITTED for transactions started using replication replay task");
        }
        /**
         * This makes the operation idempotent
         * (assume that this is most likely due to retry logic)
         */
        LOG.info("Nth commitTxn({}) msg", JavaUtils.txnIdToString(txnid));
        return null;
      }
      TxnUtils.raiseTxnUnexpectedState(actualTxnStatus, txnid);
    }

    String conflictSQLSuffix = "FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\"=" + txnid + " AND \"TC_OPERATION_TYPE\" IN (" +
        OperationType.UPDATE + "," + OperationType.DELETE + ")";
    long tempCommitId = TxnUtils.generateTemporaryId();

    if (txnType == TxnType.SOFT_DELETE || txnType == TxnType.COMPACTION) {
      new AcquireTxnLockFunction(false).execute(jdbcResource);
      commitId = jdbcResource.execute(new GetHighWaterMarkHandler());

    } else if (txnType != TxnType.READ_ONLY && !isReplayedReplTxn) {
      String writeSetInsertSql = "INSERT INTO \"WRITE_SET\" (\"WS_DATABASE\", \"WS_TABLE\", \"WS_PARTITION\"," +
          "   \"WS_TXNID\", \"WS_COMMIT_ID\", \"WS_OPERATION_TYPE\")" +
          " SELECT DISTINCT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_TXNID\", " + tempCommitId + ", \"TC_OPERATION_TYPE\" ";

      boolean isUpdateOrDelete = Boolean.TRUE.equals(jdbcResource.getJdbcTemplate().query(
          jdbcResource.getSqlGenerator().addLimitClause(1, "\"TC_OPERATION_TYPE\" " + conflictSQLSuffix),
          ResultSet::next));
      
      if (isUpdateOrDelete) {
        isUpdateDelete = 'Y';
        //if here it means currently committing txn performed update/delete and we should check WW conflict
        /**
         * "select distinct" is used below because
         * 1. once we get to multi-statement txns, we only care to record that something was updated once
         * 2. if {@link #addDynamicPartitions(AddDynamicPartitions)} is retried by caller it may create
         *  duplicate entries in TXN_COMPONENTS
         * but we want to add a PK on WRITE_SET which won't have unique rows w/o this distinct
         * even if it includes all of its columns
         *
         * First insert into write_set using a temporary commitID, which will be updated in a separate call,
         * see: {@link #updateWSCommitIdAndCleanUpMetadata(Statement, long, TxnType, Long, long)}}.
         * This should decrease the scope of the S4U lock on the next_txn_id table.
         */
        Object undoWriteSetForCurrentTxn = context.createSavepoint();
        jdbcResource.getJdbcTemplate().update(
            writeSetInsertSql + (TxnHandler.ConfVars.useMinHistoryLevel() ? conflictSQLSuffix :
            "FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\"= :txnId AND \"TC_OPERATION_TYPE\" <> :type"),
            new MapSqlParameterSource()
                .addValue("txnId", txnid)
                .addValue("type", OperationType.COMPACT.getSqlConst()));

        /**
         * This S4U will mutex with other commitTxn() and openTxns().
         * -1 below makes txn intervals look like [3,3] [4,4] if all txns are serial
         * Note: it's possible to have several txns have the same commit id.  Suppose 3 txns start
         * at the same time and no new txns start until all 3 commit.
         * We could've incremented the sequence for commitId as well but it doesn't add anything functionally.
         */
        new AcquireTxnLockFunction(false).execute(jdbcResource);
        commitId = jdbcResource.execute(new GetHighWaterMarkHandler());

        if (!rqst.isExclWriteEnabled()) {
          /**
           * see if there are any overlapping txns that wrote the same element, i.e. have a conflict
           * Since entire commit operation is mutexed wrt other start/commit ops,
           * committed.ws_commit_id <= current.ws_commit_id for all txns
           * thus if committed.ws_commit_id < current.ws_txnid, transactions do NOT overlap
           * For example, [17,20] is committed, [6,80] is being committed right now - these overlap
           * [17,20] committed and [21,21] committing now - these do not overlap.
           * [17,18] committed and [18,19] committing now - these overlap  (here 18 started while 17 was still running)
           */
          WriteSetInfo info = checkForWriteConflict(jdbcResource, txnid);
          if (info != null) {
            //found a conflict, so let's abort the txn
            String committedTxn = "[" + JavaUtils.txnIdToString(info.txnId) + "," + info.committedCommitId + "]";
            StringBuilder resource = new StringBuilder(info.database).append("/").append(info.table);
            if (info.partition != null) {
              resource.append('/').append(info.partition);
            }
            String msg = "Aborting [" + JavaUtils.txnIdToString(txnid) + "," + commitId + "]" + " due to a write conflict on " + resource +
                " committed by " + committedTxn + " " + info.currentOperationType + "/" + info.committedOperationType;
            //remove WRITE_SET info for current txn since it's about to abort
            context.rollbackToSavepoint(undoWriteSetForCurrentTxn);
            LOG.info(msg);
            //todo: should make abortTxns() write something into TXNS.TXN_META_INFO about this
            if (new AbortTxnsFunction(Collections.singletonList(txnid), false, false, 
                isReplayedReplTxn, TxnErrorMsg.ABORT_WRITE_CONFLICT).execute(jdbcResource) != 1) {
              throw new IllegalStateException(msg + " FAILED!");
            }
            throw new TxnAbortedException(msg);
          }
        }
      } else if (!TxnHandler.ConfVars.useMinHistoryLevel()) {
        jdbcResource.getJdbcTemplate().update(writeSetInsertSql + "FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" = :txnId AND \"TC_OPERATION_TYPE\" <> :type",
            new MapSqlParameterSource()
                .addValue("txnId", txnid)
                .addValue("type", OperationType.COMPACT.getSqlConst()));
        commitId = jdbcResource.execute(new GetHighWaterMarkHandler());
      }
    } else {
      /*
       * current txn didn't update/delete anything (may have inserted), so just proceed with commit
       *
       * We only care about commit id for write txns, so for RO (when supported) txns we don't
       * have to mutex on NEXT_TXN_ID.
       * Consider: if RO txn is after a W txn, then RO's openTxns() will be mutexed with W's
       * commitTxn() because both do S4U on NEXT_TXN_ID and thus RO will see result of W txn.
       * If RO < W, then there is no reads-from relationship.
       * In replication flow we don't expect any write write conflict as it should have been handled at source.
       */
      assert true;
    }


    if (txnType != TxnType.READ_ONLY && !isReplayedReplTxn && !MetaStoreServerUtils.isCompactionTxn(txnType)) {
      moveTxnComponentsToCompleted(jdbcResource, txnid, isUpdateDelete);
    } else if (isReplayedReplTxn) {
      if (rqst.isSetWriteEventInfos() && !rqst.getWriteEventInfos().isEmpty()) {
        jdbcResource.execute(new InsertCompletedTxnComponentsCommand(txnid, isUpdateDelete, rqst.getWriteEventInfos()));
      }
      jdbcResource.execute(new DeleteReplTxnMapEntryCommand(sourceTxnId, rqst.getReplPolicy()));
    }
    updateWSCommitIdAndCleanUpMetadata(jdbcResource, txnid, txnType, commitId, tempCommitId);
    jdbcResource.execute(new RemoveTxnsFromMinHistoryLevelCommand(ImmutableList.of(txnid)));
    jdbcResource.execute(new RemoveWriteIdsFromMinHistoryCommand(ImmutableList.of(txnid)));
    if (rqst.isSetKeyValue()) {
      updateKeyValueAssociatedWithTxn(jdbcResource, rqst);
    }

    if (!isHiveReplTxn) {
      createCommitNotificationEvent(jdbcResource, txnid , txnType);
    }

    LOG.debug("Going to commit");

    if (MetastoreConf.getBoolVar(jdbcResource.getConf(), MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
      Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_COMMITTED_TXNS).inc();
    }
    return txnType;
  }

  private void updateReplId(MultiDataSourceJdbcResource jdbcResource, ReplLastIdInfo replLastIdInfo) throws MetaException {
    String lastReplId = Long.toString(replLastIdInfo.getLastReplId());
    String catalog = replLastIdInfo.isSetCatalog() ? normalizeIdentifier(replLastIdInfo.getCatalog()) :
        MetaStoreUtils.getDefaultCatalog(jdbcResource.getConf());
    String db = normalizeIdentifier(replLastIdInfo.getDatabase());
    String table = replLastIdInfo.isSetTable() ? normalizeIdentifier(replLastIdInfo.getTable()) : null;
    List<String> partList = replLastIdInfo.isSetPartitionList() ? replLastIdInfo.getPartitionList() : null;

    String s = jdbcResource.getSqlGenerator().getDbProduct().getPrepareTxnStmt();
    if (s != null) {
      jdbcResource.getJdbcTemplate().execute(s, ps -> null);
    }

    // not used select for update as it will be updated by single thread only from repl load
    long dbId = updateDatabaseProp(jdbcResource, catalog, db, ReplConst.REPL_TARGET_TABLE_PROPERTY, lastReplId);
    if (table != null) {
      long tableId = updateTableProp(jdbcResource, catalog, db, dbId, table, ReplConst.REPL_TARGET_TABLE_PROPERTY, lastReplId);
      if (partList != null && !partList.isEmpty()) {
        updatePartitionProp(jdbcResource, tableId, partList, ReplConst.REPL_TARGET_TABLE_PROPERTY, lastReplId);
      }
    }
  }

  private long updateDatabaseProp(MultiDataSourceJdbcResource jdbcResource, String catalog, String database, 
                                  String prop, String propValue) throws MetaException {
    String query = 
        "SELECT d.\"DB_ID\", dp.\"PARAM_KEY\", dp.\"PARAM_VALUE\" FROM \"DATABASE_PARAMS\" dp\n" +
            "RIGHT JOIN \"DBS\" d ON dp.\"DB_ID\" = d.\"DB_ID\" " +
        "WHERE \"NAME\" = :dbName  and \"CTLG_NAME\" = :catalog";
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to execute query <" + query + ">");
    }
    DbEntityParam dbEntityParam = jdbcResource.getJdbcTemplate().query(query,
        new MapSqlParameterSource()
            .addValue("dbName", database)
            .addValue("catalog", catalog),
        //no row means database no found
        rs -> rs.next()
            ? new DbEntityParam(rs.getLong("DB_ID"), rs.getString("PARAM_KEY"), rs.getString("PARAM_VALUE"))
            : null);

    if (dbEntityParam == null) {
      throw new MetaException("DB with name " + database + " does not exist in catalog " + catalog);
    }

    //TODO: would be better to replace with MERGE or UPSERT
    String command;
    if (dbEntityParam.key == null) {
      command = "INSERT INTO \"DATABASE_PARAMS\" VALUES (:dbId, :key, :value)";
    } else if (!dbEntityParam.value.equals(propValue)) {
      command = "UPDATE \"DATABASE_PARAMS\" SET \"PARAM_VALUE\" = :value WHERE \"DB_ID\" = :dbId AND \"PARAM_KEY\" = :key";
    } else {
      LOG.info("Database property: {} with value: {} already updated for db: {}", prop, propValue, database);
      return dbEntityParam.id;      
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating {} for db: {}  using command {}", prop, database, command);
    }
    SqlParameterSource params = new MapSqlParameterSource()
        .addValue("dbId", dbEntityParam.id)
        .addValue("key", prop)
        .addValue("value", propValue);
    if (jdbcResource.getJdbcTemplate().update(command, params) != 1) {
      //only one row insert or update should happen
      throw new RuntimeException("DATABASE_PARAMS is corrupted for database: " + database);
    }
    return dbEntityParam.id;
  }

  private long updateTableProp(MultiDataSourceJdbcResource jdbcResource, String catalog, String db, long dbId,
                                  String table, String prop, String propValue) throws MetaException {
    String query = 
        "SELECT t.\"TBL_ID\", tp.\"PARAM_KEY\", tp.\"PARAM_VALUE\" FROM \"TABLE_PARAMS\" tp " +
            "RIGHT JOIN \"TBLS\" t ON tp.\"TBL_ID\" = t.\"TBL_ID\" " +
        "WHERE t.\"DB_ID\" = :dbId AND t.\"TBL_NAME\" = :tableName";
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to execute query <" + query + ">");
    }
    DbEntityParam dbEntityParam = jdbcResource.getJdbcTemplate().query(query,
        new MapSqlParameterSource()
            .addValue("tableName", table)
            .addValue("dbId", dbId),
        //no row means table no found
        rs -> rs.next() 
            ? new DbEntityParam(rs.getLong("TBL_ID"), rs.getString("PARAM_KEY"), rs.getString("PARAM_VALUE")) 
            : null);

    if (dbEntityParam == null) {
      throw new MetaException("Table with name " + table + " does not exist in db " + catalog + "." + db);
    }

    //TODO: would be better to replace with MERGE or UPSERT
    String command;
    if (dbEntityParam.key == null) {
      command = "INSERT INTO \"TABLE_PARAMS\" VALUES (:tblId, :key, :value)";
    } else if (!dbEntityParam.value.equals(propValue)) {
      command = "UPDATE \"TABLE_PARAMS\" SET \"PARAM_VALUE\" = :value WHERE \"TBL_ID\" = :dbId AND \"PARAM_KEY\" = :key";
    } else {
      LOG.info("Database property: {} with value: {} already updated for db: {}", prop, propValue, db);
      return dbEntityParam.id;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating {} for table: {}  using command {}", prop, table, command);
    }
    SqlParameterSource params = new MapSqlParameterSource()
        .addValue("tblId", dbEntityParam.id)
        .addValue("key", prop)
        .addValue("value", propValue);
    if (jdbcResource.getJdbcTemplate().update(command, params) != 1) {
      //only one row insert or update should happen
      throw new RuntimeException("TABLE_PARAMS is corrupted for table: " + table);
    }
    return dbEntityParam.id;
  }
  
  private void updatePartitionProp(MultiDataSourceJdbcResource jdbcResource, long tableId,
                                   List<String> partList, String prop, String propValue) {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();
    //language=SQL
    prefix.append(
        "SELECT p.\"PART_ID\", pp.\"PARAM_KEY\", pp.\"PARAM_VALUE\" FROM \"PARTITION_PARAMS\" pp\n" +
        "RIGHT JOIN \"PARTITIONS\" p ON pp.\"PART_ID\" = p.\"PART_ID\" WHERE p.\"TBL_ID\" = :tblId AND pp.\"PARAM_KEY\" = :key");

    // Populate the complete query with provided prefix and suffix
    TxnUtils.buildQueryWithINClauseStrings(jdbcResource.getConf(), queries, prefix, suffix, partList,
        "\"PART_NAME\"", true, false);
    SqlParameterSource params = new MapSqlParameterSource()
        .addValue("tblId", tableId)
        .addValue("key", prop);
    List<DbEntityParam> partitionParams = new ArrayList<>();
    for(String query : queries) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query <" + query + ">");
      }
      jdbcResource.getJdbcTemplate().query(query, params,
          (ResultSet rs) -> {
            while (rs.next()) {
              partitionParams.add(new DbEntityParam(rs.getLong("PART_ID"), rs.getString("PARAM_KEY"), rs.getString("PARAM_VALUE")));
            }
          });
    }

    //TODO: would be better to replace with MERGE or UPSERT
    int maxBatchSize = MetastoreConf.getIntVar(jdbcResource.getConf(), MetastoreConf.ConfVars.JDBC_MAX_BATCH_SIZE);
    //all insert in one batch
    int[][] inserts = jdbcResource.getJdbcTemplate().getJdbcTemplate().batchUpdate(
        "INSERT INTO \"PARTITION_PARAMS\" VALUES (?, ?, ?)",
        partitionParams.stream().filter(p -> p.key == null).collect(Collectors.toList()), maxBatchSize,
        (ps, argument) -> {
          ps.setLong(1, argument.id);
          ps.setString(2, argument.key);
          ps.setString(3, propValue);
        });
    //all update in one batch
    int[][] updates =jdbcResource.getJdbcTemplate().getJdbcTemplate().batchUpdate(
        "UPDATE \"PARTITION_PARAMS\" SET \"PARAM_VALUE\" = ? WHERE \"PART_ID\" = ? AND \"PARAM_KEY\" = ?",
        partitionParams.stream().filter(p -> p.key != null && !propValue.equals(p.value)).collect(Collectors.toList()), maxBatchSize,
        (ps, argument) -> {
          ps.setString(1, propValue);
          ps.setLong(2, argument.id);
          ps.setString(3, argument.key);
        });

    if (Arrays.stream(inserts).flatMapToInt(IntStream::of).sum() + Arrays.stream(updates).flatMapToInt(IntStream::of).sum() != partList.size()) {
      throw new RuntimeException("PARTITION_PARAMS is corrupted, update failed");      
    }    
  }

  private WriteSetInfo checkForWriteConflict(MultiDataSourceJdbcResource jdbcResource, long txnid) throws MetaException {
    String writeConflictQuery = jdbcResource.getSqlGenerator().addLimitClause(1, 
        "\"COMMITTED\".\"WS_TXNID\", \"COMMITTED\".\"WS_COMMIT_ID\", " +
        "\"COMMITTED\".\"WS_DATABASE\", \"COMMITTED\".\"WS_TABLE\", \"COMMITTED\".\"WS_PARTITION\", " +
        "\"CUR\".\"WS_COMMIT_ID\" \"CUR_WS_COMMIT_ID\", \"CUR\".\"WS_OPERATION_TYPE\" \"CUR_OP\", " +
        "\"COMMITTED\".\"WS_OPERATION_TYPE\" \"COMMITTED_OP\" FROM \"WRITE_SET\" \"COMMITTED\" INNER JOIN \"WRITE_SET\" \"CUR\" " +
        "ON \"COMMITTED\".\"WS_DATABASE\"=\"CUR\".\"WS_DATABASE\" AND \"COMMITTED\".\"WS_TABLE\"=\"CUR\".\"WS_TABLE\" " +
        //For partitioned table we always track writes at partition level (never at table)
        //and for non partitioned - always at table level, thus the same table should never
        //have entries with partition key and w/o
        "AND (\"COMMITTED\".\"WS_PARTITION\"=\"CUR\".\"WS_PARTITION\" OR (\"COMMITTED\".\"WS_PARTITION\" IS NULL AND \"CUR\".\"WS_PARTITION\" IS NULL)) " +
        "WHERE \"CUR\".\"WS_TXNID\" <= \"COMMITTED\".\"WS_COMMIT_ID\" " + //txns overlap; could replace ws_txnid
        // with txnid, though any decent DB should infer this
        "AND \"CUR\".\"WS_TXNID\"= :txnId " + //make sure RHS of join only has rows we just inserted as
        // part of this commitTxn() op
        "AND \"COMMITTED\".\"WS_TXNID\" <> :txnId " + //and LHS only has committed txns
        //U+U and U+D and D+D is a conflict and we don't currently track I in WRITE_SET at all
        //it may seem like D+D should not be in conflict but consider 2 multi-stmt txns
        //where each does "delete X + insert X, where X is a row with the same PK.  This is
        //equivalent to an update of X but won't be in conflict unless D+D is in conflict.
        //The same happens when Hive splits U=I+D early so it looks like 2 branches of a
        //multi-insert stmt (an Insert and a Delete branch).  It also 'feels'
        // un-serializable to allow concurrent deletes
        "AND (\"COMMITTED\".\"WS_OPERATION_TYPE\" IN(:opUpdate, :opDelete) " +
        "AND \"CUR\".\"WS_OPERATION_TYPE\" IN(:opUpdate, :opDelete))");
    LOG.debug("Going to execute query: <{}>", writeConflictQuery);
    return jdbcResource.getJdbcTemplate().query(writeConflictQuery,
        new MapSqlParameterSource()
            .addValue("txnId", txnid)
            .addValue("opUpdate", OperationType.UPDATE.getSqlConst())
            .addValue("opDelete", OperationType.DELETE.getSqlConst()),
        (ResultSet rs) -> {
          if(rs.next()) {
            return new WriteSetInfo(rs.getLong("WS_TXNID"), rs.getLong("CUR_WS_COMMIT_ID"),
                rs.getLong("WS_COMMIT_ID"), rs.getString("CUR_OP"), rs.getString("COMMITTED_OP"),
                rs.getString("WS_DATABASE"), rs.getString("WS_TABLE"), rs.getString("WS_PARTITION"));
          } else {
            return null;
          }
        });
  }

  private void moveTxnComponentsToCompleted(MultiDataSourceJdbcResource jdbcResource, long txnid, char isUpdateDelete) {
    // Move the record from txn_components into completed_txn_components so that the compactor
    // knows where to look to compact.
    String query = "INSERT INTO \"COMPLETED_TXN_COMPONENTS\" (\"CTC_TXNID\", \"CTC_DATABASE\", " +
        "\"CTC_TABLE\", \"CTC_PARTITION\", \"CTC_WRITEID\", \"CTC_UPDATE_DELETE\") SELECT \"TC_TXNID\", " +
        "\"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_WRITEID\", :flag FROM \"TXN_COMPONENTS\" " +
        "WHERE \"TC_TXNID\" = :txnid AND \"TC_OPERATION_TYPE\" <> :type";
    //we only track compactor activity in TXN_COMPONENTS to handle the case where the
    //compactor txn aborts - so don't bother copying it to COMPLETED_TXN_COMPONENTS
    LOG.debug("Going to execute insert <{}>", query);
    int affectedRows = jdbcResource.getJdbcTemplate().update(query,
        new MapSqlParameterSource()
            .addValue("flag", Character.toString(isUpdateDelete), Types.CHAR)
            .addValue("txnid", txnid)
            .addValue("type", OperationType.COMPACT.getSqlConst(), Types.CHAR));

    if (affectedRows < 1) {
      //this can be reasonable for an empty txn START/COMMIT or read-only txn
      //also an IUD with DP that didn't match any rows.
      LOG.info("Expected to move at least one record from txn_components to "
          + "completed_txn_components when committing txn! {}", JavaUtils.txnIdToString(txnid));
    }
  }

  private void updateKeyValueAssociatedWithTxn(MultiDataSourceJdbcResource jdbcResource, CommitTxnRequest rqst) {
    if (!rqst.getKeyValue().getKey().startsWith(TxnStore.TXN_KEY_START)) {
      String errorMsg = "Error updating key/value in the sql backend with"
          + " txnId=" + rqst.getTxnid() + ","
          + " tableId=" + rqst.getKeyValue().getTableId() + ","
          + " key=" + rqst.getKeyValue().getKey() + ","
          + " value=" + rqst.getKeyValue().getValue() + "."
          + " key should start with " + TxnStore.TXN_KEY_START + ".";
      LOG.warn(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }
    String query = "UPDATE \"TABLE_PARAMS\" SET \"PARAM_VALUE\" = :value WHERE \"TBL_ID\" = :id AND \"PARAM_KEY\" = :key";
    LOG.debug("Going to execute update <{}>", query);
    int affectedRows = jdbcResource.getJdbcTemplate().update(query,
        new MapSqlParameterSource()
            .addValue("value", rqst.getKeyValue().getValue())
            .addValue("id", rqst.getKeyValue().getTableId())
            .addValue("key", rqst.getKeyValue().getKey()));
    
    if (affectedRows != 1) {
      String errorMsg = "Error updating key/value in the sql backend with"
          + " txnId=" + rqst.getTxnid() + ","
          + " tableId=" + rqst.getKeyValue().getTableId() + ","
          + " key=" + rqst.getKeyValue().getKey() + ","
          + " value=" + rqst.getKeyValue().getValue() + "."
          + " Only one row should have been affected but "
          + affectedRows + " rows where affected.";
      LOG.warn(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  /**
   * See overridden method in CompactionTxnHandler also.
   */
  private void updateWSCommitIdAndCleanUpMetadata(MultiDataSourceJdbcResource jdbcResource, long txnid, TxnType txnType,
                                                    Long commitId, long tempId) throws MetaException {
    List<String> queryBatch = new ArrayList<>(6);
    // update write_set with real commitId
    if (commitId != null) {
      queryBatch.add("UPDATE \"WRITE_SET\" SET \"WS_COMMIT_ID\" = " + commitId +
          " WHERE \"WS_COMMIT_ID\" = " + tempId + " AND \"WS_TXNID\" = " + txnid);
    }
    // clean up txn related metadata
    if (txnType != TxnType.READ_ONLY) {
      queryBatch.add("DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" = " + txnid);
    }
    queryBatch.add("DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_TXNID\" = " + txnid);
    // DO NOT remove the transaction from the TXN table, the cleaner will remove it when appropriate
    queryBatch.add("UPDATE \"TXNS\" SET \"TXN_STATE\" = " + TxnStatus.COMMITTED + " WHERE \"TXN_ID\" = " + txnid);
    if (txnType == TxnType.MATER_VIEW_REBUILD) {
      queryBatch.add("DELETE FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE \"MRL_TXN_ID\" = " + txnid);
    }
    if (txnType == TxnType.SOFT_DELETE || txnType == TxnType.COMPACTION) {
      queryBatch.add("UPDATE \"COMPACTION_QUEUE\" SET \"CQ_NEXT_TXN_ID\" = " + commitId + ", \"CQ_COMMIT_TIME\" = " +
          getEpochFn(jdbcResource.getDatabaseProduct()) + " WHERE \"CQ_TXN_ID\" = " + txnid);
    }
    
    // execute all in one batch
    jdbcResource.getJdbcTemplate().getJdbcTemplate().batchUpdate(queryBatch.toArray(new String[0]));
  }

  /**
   * Create Notifiaction Events on txn commit
   * @param txnid committed txn
   * @param txnType transaction type
   * @throws MetaException ex
   */
  private void createCommitNotificationEvent(MultiDataSourceJdbcResource jdbcResource, long txnid, TxnType txnType)
      throws MetaException {
    if (transactionalListeners != null) {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
          EventMessage.EventType.COMMIT_TXN, new CommitTxnEvent(txnid, txnType), jdbcResource.getConnection(), jdbcResource.getSqlGenerator());

      CompactionInfo compactionInfo = jdbcResource.execute(new GetCompactionInfoHandler(txnid, true));
      if (compactionInfo != null) {
        MetaStoreListenerNotifier
            .notifyEventWithDirectSql(transactionalListeners, EventMessage.EventType.COMMIT_COMPACTION,
                new CommitCompactionEvent(txnid, compactionInfo), jdbcResource.getConnection(), jdbcResource.getSqlGenerator());
      } else {
        LOG.warn("No compaction queue record found for Compaction type transaction commit. txnId:" + txnid);
      }
      
    }
  }

  private static class DbEntityParam {
    final long id;
    final String key;
    final String value;

    public DbEntityParam(long id, String key, String value) {
      this.id = id;
      this.key = key;
      this.value = value;
    }
  }
  
  private static class WriteSetInfo {
    final long txnId;
    final long currentCommitId;
    final long committedCommitId;
    final String currentOperationType;
    final String committedOperationType;
    final String database;
    final String table;
    final String partition;

    public WriteSetInfo(long txnId, long currentCommitId, long committedCommitId, 
                        String currentOperationType, String committedOperationType, 
                        String database, String table, String partition) {
      this.txnId = txnId;
      this.currentCommitId = currentCommitId;
      this.committedCommitId = committedCommitId;
      this.currentOperationType = currentOperationType;
      this.committedOperationType = committedOperationType;
      this.database = database;
      this.table = table;
      this.partition = partition;
    }
  }

}