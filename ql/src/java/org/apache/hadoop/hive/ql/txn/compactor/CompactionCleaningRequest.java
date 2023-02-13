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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.util.Map;

/**
 * A cleaning request class specific to compaction based cleanup.
 */
public class CompactionCleaningRequest extends CleaningRequest {

  private final CompactionInfo compactionInfo;
  private final Map<Path, AcidUtils.HdfsDirSnapshot> dirSnapshots;

  public CompactionCleaningRequest(CompactionCleaningRequestBuilder builder) {
    super(builder);
    this.compactionInfo = builder.compactionInfo;
    this.dirSnapshots = builder.dirSnapshots;
  }

  public CompactionInfo getCompactionInfo() {
    return compactionInfo;
  }

  public Map<Path, AcidUtils.HdfsDirSnapshot> getHdfsDirSnapshots() {
    return dirSnapshots;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("Request type", getType().toString())
            .append("Compaction Info", getCompactionInfo().toString())
            .append("Obsolete directories", getObsoleteDirs())
            .append("Cleaner metric", getObsoleteDirs())
            .append("Drop partition", isDropPartition()).build();
  }

  /**
   * A builder specific to compaction cleaning request
   */
  public static class CompactionCleaningRequestBuilder extends
          CleaningRequestBuilder<CompactionCleaningRequestBuilder> {
    private CompactionInfo compactionInfo;
    private Map<Path, AcidUtils.HdfsDirSnapshot> dirSnapshots;

    public CompactionCleaningRequestBuilder setCompactionInfo(CompactionInfo compactionInfo) {
      this.compactionInfo = compactionInfo;
      return self();
    }

    public CompactionCleaningRequestBuilder setDirSnapshots(Map<Path, AcidUtils.HdfsDirSnapshot> dirSnapshots) {
      this.dirSnapshots = dirSnapshots;
      return self();
    }

    @Override
    public CompactionCleaningRequest build() {
      this.setType(RequestType.COMPACTION);
      this.setCleanerMetric(MetricsConstants.COMPACTION_CLEANER_CYCLE + "_" +
              (compactionInfo.type != null ? compactionInfo.type.toString().toLowerCase() : null));
      if (compactionInfo != null) {
        this.setDbName(compactionInfo.dbname)
                .setTableName(compactionInfo.tableName)
                .setPartitionName(compactionInfo.partName)
                .setRunAs(compactionInfo.runAs)
                .setFullPartitionName(compactionInfo.getFullPartitionName());
      }
      return new CompactionCleaningRequest(this);
    }
  }
}
