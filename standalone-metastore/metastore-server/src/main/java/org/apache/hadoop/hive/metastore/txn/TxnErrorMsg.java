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

package org.apache.hadoop.hive.metastore.txn;

/**
 * The following class represents all the error messages that are handled for aborts.
 */
public enum TxnErrorMsg {
    // Txn Errors Codes: 50000 - 59999.
    // Query runtime aborts - 50000-50999
    NONE(50000, "None"),
    ABORT_QUERY(50001, " Txn aborted by Abort Query Command"),
    ABORT_CONCURRENT(50002, " Txn aborted due to concurrent committed transaction"),
    ABORT_WRITE_CONFLICT(50003, " Txn aborted due to write conflicts"),
    ABORT_TIMEOUT(50004, " Txn aborted due to heartbeat time-out"),
    ABORT_ROLLBACK(50005, "Txn aborted due to rollback"),
    ABORT_COMPACTION_TXN(50006, "Compaction txn is aborted"),
    ABORT_MSCK_TXN(50007, "Msck txn is aborted"),
    ABORT_MIGRATION_TXN(50008, "Managed Migration transaction is aborted"),

    // Replication related aborts - 51000 - 51099
    ABORT_DEFAULT_REPL_TXN(51000, " Replication:" +
            "Abort default replication transaction"),
    ABORT_REPLAYED_REPL_TXN(51001, " Replication:" +
            "Abort replayed replication transaction"),
    ABORT_REPL_WRITEID_TXN(51002, " Replication:" +
            "Abort all the allocated txns so that the mapped write ids are referred as aborted ones."),
    ABORT_FETCH_FAILOVER_METADATA(51003, " Replication:" +
            "Abort all transactions while trying to fetch failover metadata."),
    ABORT_WRITE_TXN_AFTER_TIMEOUT(51004, " Replication:" +
            "Abort only write transactions for the db under replication"),
    ABORT_ONGOING_TXN_FOR_TARGET_DB(51005, " Replication:" +
            "Abort the ongoing transactions(opened prior to failover) for the target database.");

    private final long errorCode;
    private final String txnErrorMsg;

    TxnErrorMsg(int errorCode, String txnErrorMsg) {
      this.errorCode = errorCode;
      this.txnErrorMsg = txnErrorMsg;
    }

    public long getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
      return "TxnLog: TxnErrorMsg{" +
             "errorCode=" + errorCode +
             ", txnErrorMsg=" + txnErrorMsg  +"}";
    }

    public static TxnErrorMsg getErrorMsg(long errorCode) {
      for (TxnErrorMsg errorMsg : values()) {
        if (errorMsg.getErrorCode() == errorCode) {
          return errorMsg;
        }
      }
      return null;
    }
}

