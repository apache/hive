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

import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * Compactor threads that runs in the metastore. It uses a {@link TxnStore}
 * to access the internal database.
 */
public class MetaStoreCompactorThread extends CompactorThread implements MetaStoreThread {

  protected TxnStore txnHandler;
  protected int threadId;

  @Override
  public void setThreadId(int threadId) {
    this.threadId = threadId;
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);

    // Get our own instance of the transaction handler
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @Override Table resolveTable(CompactionInfo ci) throws MetaException {
    try {
      return getMSForConf(conf).getTable(getDefaultCatalog(conf), ci.dbname, ci.tableName);
    } catch (MetaException e) {
      LOG.error("Unable to find table " + ci.getFullTableName() + ", " + e.getMessage());
      throw e;
    }
  }

  @Override boolean replIsCompactionDisabledForDatabase(String dbName) throws TException {
    try {
      Database database = getMSForConf(conf).getDatabase(getDefaultCatalog(conf), dbName);
      // Compaction is disabled until after first successful incremental load. Check HIVE-21197 for more detail.
      boolean isReplCompactDisabled = ReplUtils.isFirstIncPending(database.getParameters());
      if (isReplCompactDisabled) {
        LOG.info("Compaction is disabled for database " + dbName);
      }
      return isReplCompactDisabled;
    } catch (NoSuchObjectException e) {
      LOG.info("Unable to find database " + dbName);
      return true;
    }
  }

  @Override List<Partition> getPartitionsByNames(CompactionInfo ci) throws MetaException {
    try {
      return getMSForConf(conf).getPartitionsByNames(getDefaultCatalog(conf), ci.dbname, ci.tableName,
          Collections.singletonList(ci.partName));
    } catch (MetaException e) {
      LOG.error("Unable to get partitions by name for CompactionInfo=" + ci);
      throw e;
    } catch (NoSuchObjectException e) {
      LOG.error("Unable to get partitions by name for CompactionInfo=" + ci);
      throw new MetaException(e.toString());
    }
  }
}
