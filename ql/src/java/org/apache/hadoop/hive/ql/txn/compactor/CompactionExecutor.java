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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class CompactionExecutor {

  static final private String CLASS_NAME = CompactionExecutor.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  
  protected final IMetaStoreClient msc;
  protected final HiveConf conf;
  protected final CompactorFactory compactorFactory;
  protected final boolean collectGenericStats;
  protected final boolean collectMrStats;
  protected boolean computeStats = false;
  
  public CompactionExecutor(HiveConf conf, IMetaStoreClient msc, CompactorFactory compactorFactory, 
      boolean collectGenericStats, boolean collectMrStats) {
    this.conf = conf;
    this.msc = msc;
    this.compactorFactory = compactorFactory;
    this.collectGenericStats = collectGenericStats;
    this.collectMrStats = collectMrStats;
  }
  
  abstract Boolean compact(Table table, CompactionInfo ci) throws InterruptedException, TException, IOException, HiveException;
  abstract public void cleanupResultDirs();

  public boolean isComputeStats() {
    return computeStats;
  }

  protected boolean isDynPartAbort(Table t, CompactionInfo ci) {
    return t.getPartitionKeys() != null && t.getPartitionKeys().size() > 0
        && ci.partName == null;
  }

  protected void failCompactionIfSetForTest() {
    if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION)) {
      throw new RuntimeException(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION.name() + "=true");
    }
  }

  protected boolean isTableSorted(StorageDescriptor sd, CompactionInfo ci) throws TException {
    // Check that the table or partition isn't sorted, as we don't yet support that.
    if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
      ci.errorMessage = "Attempt to compact sorted table " + ci.getFullTableName() + ", which is not yet supported!";
      LOG.warn(ci.errorMessage + " Compaction info: {}", ci);
      msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
      return true;
    }
    return false;
  }
}
