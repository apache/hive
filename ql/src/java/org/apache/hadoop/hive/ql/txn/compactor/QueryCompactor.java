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
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Common interface for query based compactions.
 */
abstract class QueryCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(QueryCompactor.class.getName());
  private static final String TMPDIR = "_tmp";

  /**
   * Start a query based compaction.
   * @param hiveConf hive configuration
   * @param table the table, where the compaction should run
   * @param partition the partition, where the compaction should run
   * @param storageDescriptor this is the resolved storage descriptor
   * @param writeIds valid write IDs used to filter rows while they're being read for compaction
   * @param compactionInfo provides info about the type of compaction
   * @throws IOException compaction cannot be finished.
   */
  abstract void runCompaction(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException;

  /**
   * Collection of some helper functions.
   */
  static class Util {
    /**
     * Determine if compaction can run in a specified directory.
     * @param isMajorCompaction type of compaction.
     * @param dir the delta directory
     * @param sd resolved storage descriptor
     * @return true, if compaction can run.
     */
    static boolean isEnoughToCompact(boolean isMajorCompaction, AcidUtils.Directory dir, StorageDescriptor sd) {
      int deltaCount = dir.getCurrentDirectories().size();
      int origCount = dir.getOriginalFiles().size();

      StringBuilder deltaInfo = new StringBuilder().append(deltaCount);
      boolean isEnoughToCompact;

      if (isMajorCompaction) {
        isEnoughToCompact = (origCount > 0 || deltaCount + (dir.getBaseDirectory() == null ? 0 : 1) > 1);

      } else {
        isEnoughToCompact = (deltaCount > 1);

        if (deltaCount == 2) {
          Map<String, Long> deltaByType = dir.getCurrentDirectories().stream().collect(Collectors
              .groupingBy(delta -> (delta.isDeleteDelta() ? AcidUtils.DELETE_DELTA_PREFIX : AcidUtils.DELTA_PREFIX),
                  Collectors.counting()));

          isEnoughToCompact = (deltaByType.size() != deltaCount);
          deltaInfo.append(" ").append(deltaByType);
        }
      }

      if (!isEnoughToCompact) {
        LOG.debug("Not compacting {}; current base: {}, delta files: {}, originals: {}", sd.getLocation(),
            dir.getBaseDirectory(), deltaInfo, origCount);
      }
      return isEnoughToCompact;
    }

    /**
     * Generate a random tmp path, under the provided storage.
     * @param sd storage descriptor, must be not null.
     * @return path, always not null
     */
    static String generateTmpPath(StorageDescriptor sd) {
      return sd.getLocation() + "/" + TMPDIR + "_" + UUID.randomUUID().toString();
    }
  }
}
