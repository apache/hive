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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import java.util.List;

/**
 * ReplTxnTask.
 * Used for replaying the transaction related events.
 */
public class ReplTxnTask extends Task<ReplTxnWork> {

  private static final long serialVersionUID = 1L;

  public ReplTxnTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    String replPolicy = work.getReplPolicy();
    String tableName = work.getTableName();
    ReplicationSpec replicationSpec = work.getReplicationSpec();
    if ((tableName != null) && (replicationSpec != null)) {
      Table tbl;
      try {
        tbl = Hive.get().getTable(work.getDbName(), tableName);
        if (!replicationSpec.allowReplacementInto(tbl.getParameters())) {
          // if the event is already replayed, then no need to replay it again.
          LOG.debug("ReplTxnTask: Event is skipped as it is already replayed. Event Id: " +
                  replicationSpec.getReplicationState() + "Event Type: " + work.getOperationType());
          return 0;
        }
      } catch (InvalidTableException e) {
        LOG.info("Table does not exist so, ignoring the operation as it might be a retry(idempotent) case.");
        return 0;
      } catch (HiveException e) {
        LOG.error("Get table failed with exception " + e.getMessage());
        return 1;
      }
    }

    try {
      HiveTxnManager txnManager = driverContext.getCtx().getHiveTxnManager();
      String user = UserGroupInformation.getCurrentUser().getUserName();
      switch(work.getOperationType()) {
      case REPL_OPEN_TXN:
        List<Long> txnIds = txnManager.replOpenTxn(replPolicy, work.getTxnIds(), user);
        assert txnIds.size() == work.getTxnIds().size();
        LOG.info("Replayed OpenTxn Event for policy " + replPolicy + " with srcTxn " +
                work.getTxnIds().toString() + " and target txn id " + txnIds.toString());
        return 0;
      case REPL_ABORT_TXN:
        for (long txnId : work.getTxnIds()) {
          txnManager.replRollbackTxn(replPolicy, txnId);
          LOG.info("Replayed AbortTxn Event for policy " + replPolicy + " with srcTxn " + txnId);
        }
        return 0;
      case REPL_COMMIT_TXN:
        for (long txnId : work.getTxnIds()) {
          txnManager.replCommitTxn(replPolicy, txnId);
          LOG.info("Replayed CommitTxn Event for policy " + replPolicy + " with srcTxn " + txnId);
        }
        return 0;
      case REPL_ALLOC_WRITE_ID:
        assert work.getTxnToWriteIdList() != null;
        String dbName = work.getDbName();
        List <TxnToWriteId> txnToWriteIdList = work.getTxnToWriteIdList();
        txnManager.replAllocateTableWriteIdsBatch(dbName, tableName, replPolicy, txnToWriteIdList);
        LOG.info("Replayed alloc write Id Event for repl policy: " + replPolicy + " db Name : " + dbName +
                " txnToWriteIdList: " +txnToWriteIdList.toString() + " table name: " + tableName);
        return 0;
      case REPL_WRITEID_STATE:
        txnManager.replTableWriteIdState(work.getValidWriteIdList(),
                work.getDbName(), tableName, work.getPartNames());
        LOG.info("Replicated WriteId state for DbName: " + work.getDbName()
                + " TableName: " + tableName
                + " ValidWriteIdList: " + work.getValidWriteIdList());
        return 0;
      default:
        LOG.error("Operation Type " + work.getOperationType() + " is not supported ");
        return 1;
      }
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      setException(e);
      return 1;
    }
  }

  @Override
  public StageType getType() {
    return StageType.REPL_TXN;
  }

  @Override
  public String getName() {
    return "REPL_TRANSACTION";
  }
}
