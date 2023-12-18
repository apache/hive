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

import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * Compactor thread that can run outside the metastore. It will
 * use the metastore thrift API which will default to a remote connection
 * if the thread is running outside the metastore or to a function call
 * if it's running within the metastore.
 */
public class RemoteCompactorThread extends CompactorThread {
  protected IMetaStoreClient msc;

  private String poolName;

  public String getPoolName() {
    return poolName;
  }

  public void setPoolName(String poolName) {
    this.poolName = poolName;
  }

  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    this.msc = HiveMetaStoreUtils.getHiveMetastoreClient(conf);
  }

  @Override Table resolveTable(CompactionInfo ci) throws MetaException {
    return RemoteCompactorUtil.resolveTable(conf, msc, ci);
  }

  @Override boolean replIsCompactionDisabledForDatabase(String dbName) throws TException {
    try {
      Database database = msc.getDatabase(getDefaultCatalog(conf), dbName);
      // Compaction is disabled until after first successful incremental load. Check HIVE-21197 for more detail.
      return ReplUtils.isFirstIncPending(database.getParameters());
    } catch (NoSuchObjectException e) {
      LOG.info("Unable to find database " + dbName);
      return true;
    }
  }

  @Override List<Partition> getPartitionsByNames(CompactionInfo ci) throws MetaException {
    return RemoteCompactorUtil.getPartitionsByNames(msc, ci.dbname, ci.tableName, ci.tableName);
  }

  protected Partition resolvePartition(CompactionInfo ci) throws MetaException {
    return CompactorUtil.resolvePartition(conf, msc, ci.dbname, ci.tableName, ci.partName, 
        CompactorUtil.METADATA_FETCH_MODE.REMOTE);
  }
}
