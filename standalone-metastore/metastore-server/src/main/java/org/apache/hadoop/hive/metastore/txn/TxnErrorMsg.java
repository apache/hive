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
    NONE(50000, "none"),
    ABORT_QUERY(50001, "abort by query command"),
    ABORT_CONCURRENT(50002, "concurrent committed transaction"),
    ABORT_WRITE_CONFLICT(50003, "write conflicts"),
    ABORT_TIMEOUT(50004, "heartbeat time-out"),
    ABORT_ROLLBACK(50005, "rollback"),
    ABORT_COMPACTION_TXN(50006, "compaction transaction abort"),
    ABORT_MSCK_TXN(50007, "msck transaction abort"),
    ABORT_MIGRATION_TXN(50008, "managed migration transaction abort"),

    // Replication related aborts - 51000 - 51099
    ABORT_DEFAULT_REPL_TXN(51000, "Replication:" +
            "default replication transaction abort"),
    ABORT_REPLAYED_REPL_TXN(51001, "Replication:" +
            "replayed replication transaction abort"),
    ABORT_REPL_WRITEID_TXN(51002, "Replication:" +
            "abort of allocated txns for referring mapped write ids as aborted ones"),
    ABORT_FETCH_FAILOVER_METADATA(51003, "Replication:" +
            "abort of txns while trying to fetch failover metadata"),
    ABORT_WRITE_TXN_AFTER_TIMEOUT(51004, "Replication:" +
            "abort of write txns for the db under replication"),
    ABORT_ONGOING_TXN_FOR_TARGET_DB(51005, "Replication:" +
            "abort of ongoing txns(opened prior to failover) for the target database");

    private final long errorCode;
    private final String errorMsg;

    TxnErrorMsg(int errorCode, String errorMsg) {
      this.errorCode = errorCode;
      this.errorMsg = errorMsg;
    }

    public long getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
      return errorMsg;
    }

    public static TxnErrorMsg getTxnErrorMsg(long errorCode) {
      for (TxnErrorMsg txnErrorMsg : values()) {
        if (txnErrorMsg.getErrorCode() == errorCode) {
          return txnErrorMsg;
        }
      }
      return TxnErrorMsg.NONE;
    }

    public String toSqlString() {
      return "'" + this.toString() + "'";
    }
}

