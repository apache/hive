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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.stats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the partition statistics Iceberg computes of its own - a row count and a file count per
 * partition - as {@link IcebergColStatsReader} reads the column statistics Hive writes.
 */
public final class IcebergPartitionStatsReader {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergPartitionStatsReader.class);

  private IcebergPartitionStatsReader() {
  }

  /**
   * Reads the given snapshot's partition statistics file in a single pass, keyed by partition name;
   * empty when the file is missing.
   */
  public static Map<String, PartitionStatistics> read(Table table, Snapshot snapshot) {
    PartitionStatisticsFile statsFile = IcebergStoredStats.getPartitionStatsFile(table, snapshot.snapshotId());
    if (statsFile == null) {
      LOG.warn("Partition stats file not found for snapshot: {}", snapshot.snapshotId());
      return Map.of();
    }
    Map<String, PartitionStatistics> result = Maps.newHashMap();
    Types.StructType partitionType = Partitioning.partitionType(table);

    try (CloseableIterable<PartitionStatistics> records =
        table.newPartitionStatisticsScan().useSnapshot(snapshot.snapshotId()).scan()) {
      LOG.info("Using partition stats from: {}", statsFile.path());
      for (PartitionStatistics partitionStats : records) {
        PartitionSpec spec = table.specs().get(partitionStats.specId());
        PartitionData data = IcebergTableUtil.toPartitionData(partitionStats.partition(), partitionType,
            spec.partitionType());
        // the scan copies the counters into a fresh object per row, so retaining it is safe;
        // only the partition tuple may alias the reader's reused struct - do not use it after this loop
        result.put(IcebergTableUtil.toPartitionName(spec, data), partitionStats);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Collections.unmodifiableMap(result);
  }
}
