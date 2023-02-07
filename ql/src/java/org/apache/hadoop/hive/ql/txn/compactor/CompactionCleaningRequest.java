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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.util.List;
import java.util.Map;

/**
 * A cleaning request class specific to compaction based cleanup.
 */
public class CompactionCleaningRequest extends CleaningRequest {

  private final CompactionInfo compactionInfo;
  private final Map<Path, AcidUtils.HdfsDirSnapshot> dirSnapshots;

  public CompactionCleaningRequest(String location, CompactionInfo info, List<Path> obsoleteDirs,
                                   boolean purge, FileSystem fs, Map<Path, AcidUtils.HdfsDirSnapshot> dirSnapshots,
                                   boolean dropPartition) {
    super(RequestType.COMPACTION, location, obsoleteDirs, purge, fs);
    this.compactionInfo = info;
    this.dbName = compactionInfo.dbname;
    this.tableName = compactionInfo.tableName;
    this.partitionName = compactionInfo.partName;
    this.dirSnapshots = dirSnapshots;
    this.cleanerMetric = MetricsConstants.COMPACTION_CLEANER_CYCLE + "_" +
            (compactionInfo.type != null ? compactionInfo.type.toString().toLowerCase() : null);
    this.runAs = compactionInfo.runAs;
    this.dropPartition = dropPartition;
    this.fullPartitionName = compactionInfo.getFullPartitionName();
  }

  public CompactionInfo getCompactionInfo() {
    return compactionInfo;
  }

  public Map<Path, AcidUtils.HdfsDirSnapshot> getHdfsDirSnapshots() {
    return dirSnapshots;
  }

  @Override
  public String toString() {
    return String.join("Compaction cleaning request: [Request Type: ", this.getType().toString(),
            ", Compaction Info: [", this.compactionInfo.toString(),
            "], Obsolete directories: ", CompactorUtil.getDebugInfo(this.getObsoleteDirs()),
            ", Cleaner metric: ", this.cleanerMetric,
            ", Drop partition: ", Boolean.toString(this.dropPartition));

  }
}
