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
package org.apache.hadoop.hive.ql.txn.compactor.service;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorFactory;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class CompactionService {

  static final private String CLASS_NAME = CompactionService.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  
  protected IMetaStoreClient msc;
  protected HiveConf conf;
  protected CompactorFactory compactorFactory;
  protected boolean collectGenericStats;
  protected boolean computeStats = false;

  public CompactionService(HiveConf conf, IMetaStoreClient msc, CompactorFactory compactorFactory,
      boolean collectGenericStats) {
    init(conf, msc, compactorFactory, collectGenericStats);
  }

  public CompactionService() {
  }

  public void init(HiveConf conf, IMetaStoreClient msc, CompactorFactory compactorFactory,
      boolean collectGenericStats) {
    this.conf = conf;
    this.msc = msc;
    this.compactorFactory = compactorFactory;
    this.collectGenericStats = collectGenericStats;
  }

  public abstract Boolean compact(Table table, CompactionInfo ci) throws Exception;
  abstract public void cleanupResultDirs(CompactionInfo ci);

  public boolean isComputeStats() {
    return computeStats;
  }

  protected boolean isDynPartAbort(Table t, CompactionInfo ci) {
    return t.getPartitionKeys() != null && t.getPartitionKeys().size() > 0
        && ci.partName == null;
  }

  protected void failCompactionIfSetForTest() {
    if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION)) {
      throw new RuntimeException(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION.name() + "=true");
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
