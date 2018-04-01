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

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
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
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Executing ReplTxnTask " + work.getOperationType().toString() +
              " for txn ids : " + work.getTxnIds().toString() + " replPolicy : " + replPolicy);
    }
    try {
      HiveTxnManager txnManager = driverContext.getCtx().getHiveTxnManager();
      String user = UserGroupInformation.getCurrentUser().getUserName();
      LOG.debug("Replaying " + work.getOperationType().toString() + " Event for policy " +
              replPolicy + " with srcTxn " + work.getTxnIds().toString());
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
