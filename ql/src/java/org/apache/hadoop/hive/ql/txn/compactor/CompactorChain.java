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

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs different compactions based on the order provided in the list.
 * Mainly used for fall back mechanism for Merge compaction.
 */
final class CompactorChain implements Compactor {

  private final List<Compactor> compactors = new ArrayList<>();

  CompactorChain(List<Compactor> compactors) {
    this.compactors.addAll(compactors);
  }

  @Override
  public boolean run(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor, ValidWriteIdList writeIds, CompactionInfo compactionInfo, AcidDirectory dir) throws IOException, HiveException, InterruptedException {
    if (!compactors.isEmpty()) {
      int index = 0;
      boolean result;
      do {
        result = compactors.get(index).run(hiveConf, table, partition, storageDescriptor, writeIds, compactionInfo, dir);
        index++;
      } while (index < compactors.size() && !result);
      return true;
    } else {
      return false;
    }
  }
}
