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
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * Compactor threads that runs in the metastore. It uses a {@link TxnStore}
 * to access the internal database.
 */
public class MetaStoreCompactorThread extends CompactorThread implements MetaStoreThread {

  protected TxnStore txnHandler;
  protected RawStore rs;
  protected int threadId;

  @Override
  public void setThreadId(int threadId) {
    this.threadId = threadId;
  }

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws Exception {
    super.init(stop, looped);

    // Get our own instance of the transaction handler
    txnHandler = TxnUtils.getTxnStore(conf);

    // Get our own connection to the database so we can get table and partition information.
    rs = RawStoreProxy.getProxy(conf, conf,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.RAW_STORE_IMPL), threadId);
  }

  @Override Table resolveTable(CompactionInfo ci) throws MetaException {
    try {
      return rs.getTable(getDefaultCatalog(conf), ci.dbname, ci.tableName);
    } catch (MetaException e) {
      LOG.error("Unable to find table " + ci.getFullTableName() + ", " + e.getMessage());
      throw e;
    }
  }

  @Override List<Partition> getPartitionsByNames(CompactionInfo ci) throws MetaException {
    try {
      return rs.getPartitionsByNames(getDefaultCatalog(conf), ci.dbname, ci.tableName,
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
