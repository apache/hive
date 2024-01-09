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

import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReplTableWriteIdStateFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(ReplTableWriteIdStateFunction.class);
  
  private final ReplTblWriteIdStateRequest rqst;
  private final TxnStore.MutexAPI mutexAPI;
  private final List<TransactionalMetaStoreEventListener> transactionalListeners;

  public ReplTableWriteIdStateFunction(ReplTblWriteIdStateRequest rqst, TxnStore.MutexAPI mutexAPI, List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.rqst = rqst;
    this.mutexAPI = mutexAPI;
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    long openTxnTimeOutMillis = MetastoreConf.getTimeVar(jdbcResource.getConf(), MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS);

    String dbName = rqst.getDbName().toLowerCase();
    String tblName = rqst.getTableName().toLowerCase();
    ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(rqst.getValidWriteIdlist());

    NamedParameterJdbcTemplate npjdbcTemplate = jdbcResource.getJdbcTemplate();
    // Check if this txn state is already replicated for this given table. If yes, then it is
    // idempotent case and just return.
    boolean found = Boolean.TRUE.equals(npjdbcTemplate.query(
        "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = :dbName AND \"NWI_TABLE\" = :tableName", 
        new MapSqlParameterSource()
            .addValue("dbName", dbName)
            .addValue("tableName", tblName),
        ResultSet::next
    ));

    if (found) {
      LOG.info("Idempotent flow: WriteId state <{}> is already applied for the table: {}.{}",
          validWriteIdList, dbName, tblName);
      return null;
    }

    // Get the abortedWriteIds which are already sorted in ascending order.
    List<Long> abortedWriteIds = getAbortedWriteIds(validWriteIdList);
    int numAbortedWrites = abortedWriteIds.size();
    if (numAbortedWrites > 0) {
      // Allocate/Map one txn per aborted writeId and abort the txn to mark writeid as aborted.
      // We don't use the txnLock, all of these transactions will be aborted in this one rdbm transaction
      // So they will not effect the commitTxn in any way

      List<Long> txnIds = new OpenTxnsFunction(
          new OpenTxnRequest(numAbortedWrites, rqst.getUser(), rqst.getHostName()),
          openTxnTimeOutMillis, transactionalListeners).execute(jdbcResource);
      assert (numAbortedWrites == txnIds.size());

      // Map each aborted write id with each allocated txn.
      List<Object[]> params = new ArrayList<>(txnIds.size());
      for (int i = 0; i < txnIds.size(); i++) {
        params.add(new Object[] {txnIds.get(i), dbName, tblName, abortedWriteIds.get(i)});
        LOG.info("Allocated writeID: {} for txnId: {}", abortedWriteIds.get(i), txnIds.get(i));
      }
      
      int maxBatchSize = MetastoreConf.getIntVar(jdbcResource.getConf(), MetastoreConf.ConfVars.JDBC_MAX_BATCH_SIZE);
      jdbcResource.getJdbcTemplate().getJdbcTemplate().batchUpdate(
          "INSERT INTO \"TXN_TO_WRITE_ID\" (\"T2W_TXNID\", \"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\") VALUES (?, ?, ?, ?)",
          params, maxBatchSize, (PreparedStatement ps, Object[] statementParams) -> {
            ps.setLong(1, (Long)statementParams[0]);
            ps.setString(2, statementParams[1].toString());
            ps.setString(3, statementParams[2].toString());
            ps.setLong(4, (Long)statementParams[3]);
          });

      // Abort all the allocated txns so that the mapped write ids are referred as aborted ones.
      int numAborts = new AbortTxnsFunction(txnIds, false, false,false,
          TxnErrorMsg.ABORT_REPL_WRITEID_TXN).execute(jdbcResource);
      assert (numAborts == numAbortedWrites);
    }

    // There are some txns in the list which has no write id allocated and hence go ahead and do it.
    // Get the next write id for the given table and update it with new next write id.
    // It is expected NEXT_WRITE_ID doesn't have entry for this table and hence directly insert it.
    long nextWriteId = validWriteIdList.getHighWatermark() + 1;

    // First allocation of write id (hwm+1) should add the table to the next_write_id meta table.
    npjdbcTemplate.update(
        "INSERT INTO \"NEXT_WRITE_ID\" (\"NWI_DATABASE\", \"NWI_TABLE\", \"NWI_NEXT\") VALUES (:dbName, :tableName, :nextWriteId)",
        new MapSqlParameterSource()
            .addValue("dbName", dbName)
            .addValue("tableName", tblName)
            .addValue("nextWriteId", nextWriteId));
    LOG.info("WriteId state <{}> is applied for the table: {}.{}", validWriteIdList, dbName, tblName);

    // Schedule Major compaction on all the partitions/table to clean aborted data
    if (numAbortedWrites > 0) {
      CompactionRequest compactRqst = new CompactionRequest(rqst.getDbName(), rqst.getTableName(),
          CompactionType.MAJOR);
      if (rqst.isSetPartNames()) {
        for (String partName : rqst.getPartNames()) {
          compactRqst.setPartitionname(partName);
          new CompactFunction(compactRqst, openTxnTimeOutMillis, mutexAPI).execute(jdbcResource);
        }
      } else {
        new CompactFunction(compactRqst, openTxnTimeOutMillis, mutexAPI).execute(jdbcResource);
      }
    }
    return null;
  }

  private List<Long> getAbortedWriteIds(ValidWriteIdList validWriteIdList) {
    return Arrays.stream(validWriteIdList.getInvalidWriteIds())
        .filter(validWriteIdList::isWriteIdAborted)
        .boxed()
        .collect(Collectors.toList());
  }
  
}
