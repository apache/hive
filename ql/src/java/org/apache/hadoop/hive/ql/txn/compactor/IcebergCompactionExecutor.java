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
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IcebergCompactionExecutor extends CompactionExecutor {
  static final private String CLASS_NAME = IcebergCompactionExecutor.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final long DEFAULT_TXN_ID = 0;

  public IcebergCompactionExecutor(HiveConf conf, IMetaStoreClient msc, CompactorFactory compactorFactory, 
      boolean collectGenericStats) {
    super(conf, msc, compactorFactory, collectGenericStats);
  }

  public Boolean compact(Table table, CompactionInfo ci) throws InterruptedException, TException, IOException, HiveException {

    // Find the appropriate storage descriptor
    final StorageDescriptor sd =  CompactorUtil.resolveStorageDescriptor(table);

    if (isTableSorted(sd, ci)) {
      return false;
    }

    if (ci.runAs == null) {
      ci.runAs = TxnUtils.findUserToRunAs(sd.getLocation(), table, conf);
    }

    CompactorUtil.checkInterrupt(CLASS_NAME);
    
    msc.updateCompactorState(CompactionInfo.compactionInfoToStruct(ci), DEFAULT_TXN_ID);

    // Don't start compaction or cleaning if not necessary
    if (isDynPartAbort(table, ci)) {
      msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
      return false;
    }

    if (!ci.isMajorCompaction()) {
      ci.errorMessage = "Presently Iceberg tables support only Major compaction";
      LOG.error(ci.errorMessage + " Compaction info: {}", ci);
      try {
        msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
      } catch (Throwable tr) {
        LOG.error("Caught an exception while trying to mark compaction {} as failed: {}", ci, tr);
      }
      return false;
    }
    CompactorUtil.checkInterrupt(CLASS_NAME);

    try {
      failCompactionIfSetForTest();
      
      CompactorPipeline compactorPipeline = compactorFactory.getCompactorPipeline(table, conf, ci, msc);
      computeStats = collectGenericStats;

      LOG.info("Starting " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + ", id:" +
              ci.id + " with compute stats set to " + computeStats);

      CompactorContext compactorContext = new CompactorContext(conf, table, sd, ci);
      compactorPipeline.execute(compactorContext);

      LOG.info("Completed " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() 
          + ", marking as compacted.");
      msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
      
    } catch (Throwable e) {
      computeStats = false;
      throw e;
    }

    return true;
  }

  @Override
  public void cleanupResultDirs(CompactionInfo ci) {
    
  }
}
