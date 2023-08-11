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

package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.ddl.table.storage.compact.AlterTableCompactDesc;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.ql.io.AcidUtils.compactionTypeStr2ThriftType;

public class InitiatorForTable extends Initiator {
  public InitiatorForTable(Table table, List<Partition> partitions, AlterTableCompactDesc desc, HiveConf hiveConf)
      throws Exception {
    setConf(hiveConf);
    AtomicBoolean flag = new AtomicBoolean();
    init(flag);
    partitions.parallelStream().forEach(partition -> {
      try {
        ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(
            txnHandler.getOpenTxns(), 0);
        conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
        StorageDescriptor sd = resolveStorageDescriptor(table.getTTable(), partition.getTPartition());
        String runAs = TxnUtils.findUserToRunAs(sd.getLocation(), table.getTTable(), conf);
        CompactionInfo ci = new CompactionInfo(table.getDbName(), table.getTableName(), partition.getName(),
            compactionTypeStr2ThriftType(desc.getCompactionType()));
        ci.initiatorId = JavaUtils.hostname() + "-" + HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION;
        scheduleCompactionIfRequired(ci, table.getTTable(), partition.getTPartition(), desc.getPoolName(), runAs, false);
      } catch (IOException | InterruptedException | SemanticException | MetaException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
