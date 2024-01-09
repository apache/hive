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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorFactory;

public class CompactionExecutorFactory {

  private static final String ICEBERG_COMPACTION_SERVICE_CLASS = "org.apache.iceberg.mr.hive.compaction.IcebergCompactionService";
  
  public static CompactionService createExecutor(HiveConf conf, IMetaStoreClient msc, CompactorFactory compactorFactory,
      Table table, boolean collectGenericStats, boolean collectMrStats) throws HiveException {

    CompactionService compactionService;

    if (MetaStoreUtils.isIcebergTable(table.getParameters())) {

      try {
        Class<? extends CompactionService> icebergCompactionService = (Class<? extends CompactionService>)
            Class.forName(ICEBERG_COMPACTION_SERVICE_CLASS, true,
                Utilities.getSessionSpecifiedClassLoader());

        compactionService = icebergCompactionService.newInstance();
        compactionService.init(conf, msc, compactorFactory, collectGenericStats);
      }
      catch (Exception e) {
        throw new HiveException("Failed instantiating and calling Iceberg compaction executor", e);
      }
    }
    else {
      compactionService = new AcidCompactionService(conf, msc, compactorFactory, collectGenericStats, 
          collectMrStats);
    }

    return compactionService;
  }
}
