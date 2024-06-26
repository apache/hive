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

package org.apache.iceberg.mr.hive.compaction;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorPipeline;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.apache.hadoop.hive.ql.txn.compactor.service.CompactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCompactionService extends CompactionService {
  public static final String PARTITION_SPEC_ID = "compaction_part_spec_id";
  public static final String PARTITION_PATH = "compaction_partition_path";
  private static final String CLASS_NAME = IcebergCompactionService.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  public IcebergCompactionService() {
  }

  public Boolean compact(Table table, CompactionInfo ci) throws Exception {

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

    if (ci.runAs == null) {
      ci.runAs = TxnUtils.findUserToRunAs(table.getSd().getLocation(), table, conf);
    }

    try {
      CompactorPipeline compactorPipeline = compactorFactory.getCompactorPipeline(table, conf, ci, msc);
      computeStats = collectGenericStats;

      LOG.info("Starting " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + ", id:" +
              ci.id + " with compute stats set to " + computeStats);

      CompactorContext compactorContext = new CompactorContext(conf, table, ci);
      compactorPipeline.execute(compactorContext);

      LOG.info("Completed " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() +
          ", marking as compacted.");
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
