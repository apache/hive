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

package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ClearDanglingTxnTask.
 * clean up repl_txn_map entries that are not present in the source and roll back those dangling transactions
 **/
public class ClearDanglingTxnTask extends Task<ClearDanglingTxnWork> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClearDanglingTxnTask.class);

    @Override
    public int execute() {
        HiveTxnManager txnManager = context.getHiveTxnManager();

        String replPolicy = work.getTargetDbName().toLowerCase() + ".*";
        // Query repl_txn_map to get the replayed transactions mapping for the current policy.
        Map<String, String> replTxnEntries;
        try {
            replTxnEntries = txnManager.getReplayedTxnsForPolicy(replPolicy);
        } catch (LockException e) {
            throw new ReplicationTxnException("Failed to get entries from repl_txn_map: " + e.getMessage(), e);
        }

        if (conf.getBoolVar(HiveConf.ConfVars.HIVE_REPL_CLEAR_DANGLING_TXNS_ON_TARGET)) {
            try {
                clearDanglingTxns(replTxnEntries, txnManager, replPolicy);
            } catch (IOException | LockException e) {
                throw new ReplicationTxnException("Failed to clear dangling transactions from repl_txn_map: " + e.getMessage(), e);
            }
        } else {
            Path path = new Path(work.getDumpDirectory(), ReplUtils.OPEN_TXNS);
            FileSystem fs;
            try {
                fs = path.getParent().getFileSystem(conf);
                if (fs.exists(path)) {
                    LOG.warn("hive.repl.clear.dangling.txns.on.target is set to false in Repl Load. "
                            + "Skipping clearing dangling transactions from repl_txn_map.");
                }
            } catch (IOException e) {
                throw new ReplicationTxnException("Failed to access filesystem for replication operation: " + e.getMessage(), e);
            }
        }
        return 0;
    }


    private void clearDanglingTxns(Map<String, String> replTxnEntries, HiveTxnManager txnManager, String replPolicy) throws IOException, LockException {
        Path path = new Path(work.getDumpDirectory(), ReplUtils.OPEN_TXNS);
        FileSystem fs = path.getParent().getFileSystem(conf);
        if (!fs.exists(path)) {
            LOG.warn("Path {} does not exist. Skipping clearing dangling transactions from repl_txn_map.", path);
            return;
        }

        Set<Long> openTxnIDsFromSource = EximUtil.readAsLong(fs, path, ",");

        // Remove the open transactions from the set that are still open in the source.
        // The remaining transactions in replTxnEntries are the dangling transactions that need to be rolled back at the end of repl load.
        Set<Long> sourceTxnIdsFromReplTxnMap = replTxnEntries.keySet()
                .stream()
                .map(Long::valueOf)
                .collect(Collectors.toSet());
        sourceTxnIdsFromReplTxnMap.removeAll(openTxnIDsFromSource);

        for (long danglingTxn : sourceTxnIdsFromReplTxnMap) {
            LOG.info("Rolling back target transaction for source txn {} and policy {} ", danglingTxn, replPolicy);
            txnManager.replRollbackTxn(replPolicy, danglingTxn);
        }
    }

    @Override
    public StageType getType() {
        return StageType.CLEAR_DANGLING_TXNS;
    }

    @Override
    public String getName() {
        return "CLEAR_DANGLING_TXNS";
    }

    @Override
    public boolean canExecuteInParallel() {
        // CLEAR_DANGLING_TXNS is executed only when all its parents are done with execution.
        // So running it in parallel has no
        // benefits.
        // CLEAR_DANGLING_TXNS must be executed only when all its parents are done with execution.
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Exception thrown when replication transaction operations fail.
     */
    public static class ReplicationTxnException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public ReplicationTxnException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}