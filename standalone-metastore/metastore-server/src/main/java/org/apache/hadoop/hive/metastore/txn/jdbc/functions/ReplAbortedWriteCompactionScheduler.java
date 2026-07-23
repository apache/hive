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

import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.txn.TxnStore;

/**
 * Schedules major compactions after replication write-id state is committed.
 * Each compaction is enqueued via {@link TxnStore#compact(CompactionRequest)}, which runs in its
 * own transaction, avoiding cross-pool deadlocks when multiple partitions are processed.
 */
public final class ReplAbortedWriteCompactionScheduler {

  private ReplAbortedWriteCompactionScheduler() {}

  public static void scheduleMajorCompactions(TxnStore txnStore, ReplTblWriteIdStateRequest rqst)
      throws MetaException {
    CompactionRequest compactRqst = new CompactionRequest(rqst.getDbName(), rqst.getTableName(),
        CompactionType.MAJOR);
    if (rqst.isSetPartNames()) {
      for (String partName : rqst.getPartNames()) {
        compactRqst.setPartitionname(partName);
        txnStore.compact(compactRqst);
      }
    } else {
      txnStore.compact(compactRqst);
    }
  }
}
