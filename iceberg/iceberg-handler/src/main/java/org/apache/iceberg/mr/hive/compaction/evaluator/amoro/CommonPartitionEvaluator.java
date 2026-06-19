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

package org.apache.iceberg.mr.hive.compaction.evaluator.amoro;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonPartitionEvaluator implements PartitionEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(CommonPartitionEvaluator.class);

  private final Set<String> deleteFileSet = Sets.newHashSet();
  protected final TableRuntime tableRuntime;

  private final Pair<Integer, StructLike> partition;
  protected final OptimizingConfig config;
  protected final long fragmentSize;
  protected final long minTargetSize;
  protected final long planTime;

  private final boolean reachFullInterval;

  // fragment files
  protected int fragmentFileCount = 0;
  protected long fragmentFileSize = 0;

  // segment files
  protected int rewriteSegmentFileCount = 0;
  protected long rewriteSegmentFileSize = 0L;
  protected int undersizedSegmentFileCount = 0;
  protected long undersizedSegmentFileSize = 0;
  protected int rewritePosSegmentFileCount = 0;
  protected int combinePosSegmentFileCount = 0;
  protected long rewritePosSegmentFileSize = 0L;
  protected long min1SegmentFileSize = Integer.MAX_VALUE;
  protected long min2SegmentFileSize = Integer.MAX_VALUE;

  // delete files
  protected int equalityDeleteFileCount = 0;
  protected long equalityDeleteFileSize = 0L;
  protected int posDeleteFileCount = 0;
  protected long posDeleteFileSize = 0L;

  private long cost = -1;
  private Boolean necessary = null;
  private OptimizingType optimizingType = null;
  private String name;

  public CommonPartitionEvaluator(
      TableRuntime tableRuntime, Pair<Integer, StructLike> partition, long planTime) {
    this.partition = partition;
    this.tableRuntime = tableRuntime;
    this.config = tableRuntime.getOptimizingConfig();
    this.fragmentSize = config.getTargetSize() / config.getFragmentRatio();
    this.minTargetSize = (long) (config.getTargetSize() * config.getMinTargetSizeRatio());
    if (minTargetSize > config.getTargetSize() - fragmentSize) {
      LOG.warn(
          "The self-optimizing.min-target-size-ratio is set too large, some segment files will not be able to find " +
          "the another merge file.");
    }
    this.planTime = planTime;
    this.reachFullInterval =
        config.getFullTriggerInterval() >= 0 && planTime - tableRuntime.getLastFullOptimizingTime() >
            config.getFullTriggerInterval();
  }

  @Override
  public Pair<Integer, StructLike> getPartition() {
    return partition;
  }

  protected boolean isFragmentFile(DataFile dataFile) {
    return dataFile.fileSizeInBytes() <= fragmentSize;
  }

  protected boolean isUndersizedSegmentFile(DataFile dataFile) {
    return dataFile.fileSizeInBytes() > fragmentSize && dataFile.fileSizeInBytes() <= minTargetSize;
  }

  @Override
  public boolean addFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    if (!config.isEnabled()) {
      return false;
    }
    if (isFragmentFile(dataFile)) {
      return addFragmentFile(dataFile, deletes);
    } else if (isUndersizedSegmentFile(dataFile)) {
      return addUndersizedSegmentFile(dataFile, deletes);
    } else {
      return addTargetSizeReachedFile(dataFile, deletes);
    }
  }

  private boolean isDuplicateDelete(ContentFile<?> delete) {
    boolean deleteExist = deleteFileSet.contains(delete.path().toString());
    if (!deleteExist) {
      deleteFileSet.add(delete.path().toString());
    }
    return deleteExist;
  }

  private boolean addFragmentFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    fragmentFileSize += dataFile.fileSizeInBytes();
    fragmentFileCount++;

    for (ContentFile<?> delete : deletes) {
      addDelete(delete);
    }
    return true;
  }

  private boolean addUndersizedSegmentFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    // Because UndersizedSegment can determine whether it is rewritten during the split task stage.
    // So the calculated posDeleteFileCount, posDeleteFileSize, equalityDeleteFileCount,
    // equalityDeleteFileSize are not accurate
    for (ContentFile<?> delete : deletes) {
      addDelete(delete);
    }
    if (fileShouldRewrite(dataFile, deletes)) {
      rewriteSegmentFileSize += dataFile.fileSizeInBytes();
      rewriteSegmentFileCount++;
      return true;
    }

    // Cache the size of the smallest two files
    if (dataFile.fileSizeInBytes() < min1SegmentFileSize) {
      min2SegmentFileSize = min1SegmentFileSize;
      min1SegmentFileSize = dataFile.fileSizeInBytes();
    } else if (dataFile.fileSizeInBytes() < min2SegmentFileSize) {
      min2SegmentFileSize = dataFile.fileSizeInBytes();
    }

    undersizedSegmentFileSize += dataFile.fileSizeInBytes();
    undersizedSegmentFileCount++;
    return true;
  }

  private boolean addTargetSizeReachedFile(DataFile dataFile, List<ContentFile<?>> deletes) {
    if (fileShouldRewrite(dataFile, deletes)) {
      rewriteSegmentFileSize += dataFile.fileSizeInBytes();
      rewriteSegmentFileCount++;
      for (ContentFile<?> delete : deletes) {
        addDelete(delete);
      }
      return true;
    }

    if (segmentShouldRewritePos(dataFile, deletes)) {
      rewritePosSegmentFileSize += dataFile.fileSizeInBytes();
      rewritePosSegmentFileCount++;
      for (ContentFile<?> delete : deletes) {
        addDelete(delete);
      }
      return true;
    }

    return false;
  }

  protected boolean fileShouldFullOptimizing(DataFile dataFile, List<ContentFile<?>> deleteFiles) {
    if (config.isFullRewriteAllFiles()) {
      return true;
    }
    // If a file is related any delete files or is not big enough, it should full optimizing
    return !deleteFiles.isEmpty() || isFragmentFile(dataFile) || isUndersizedSegmentFile(dataFile);
  }

  public boolean fileShouldRewrite(DataFile dataFile, List<ContentFile<?>> deletes) {
    if (isFullOptimizing()) {
      return fileShouldFullOptimizing(dataFile, deletes);
    }
    if (isFragmentFile(dataFile)) {
      return true;
    }
    // When Upsert writing is enabled in the Flink engine, both INSERT and UPDATE_AFTER will
    // generate deletes files (Most are eq-delete), and eq-delete file will be associated
    // with the data file before the current snapshot.
    // The eq-delete does not accurately reflect how much data has been deleted in the current
    // segment file (That is, whether the segment file needs to be rewritten).
    // And the eq-delete file will be converted to pos-delete during minor optimizing, so only
    // pos-delete record count is calculated here.
    return getPosDeletesRecordCount(deletes) > dataFile.recordCount() * config.getMajorDuplicateRatio();
  }

  public boolean segmentShouldRewritePos(DataFile dataFile, List<ContentFile<?>> deletes) {
    Preconditions.checkArgument(!isFragmentFile(dataFile), "Unsupported fragment file.");
    if (deletes.stream().filter(delete -> delete.content() == FileContent.POSITION_DELETES).count() >= 2) {
      combinePosSegmentFileCount++;
      return true;
    }
    return deletes.stream().anyMatch(delete -> delete.content() == FileContent.EQUALITY_DELETES);
  }

  protected boolean isFullOptimizing() {
    return reachFullInterval();
  }

  private long getPosDeletesRecordCount(List<ContentFile<?>> files) {
    return files.stream()
        .filter(file -> file.content() == FileContent.POSITION_DELETES)
        .mapToLong(ContentFile::recordCount)
        .sum();
  }

  private void addDelete(ContentFile<?> delete) {
    if (isDuplicateDelete(delete)) {
      return;
    }
    if (delete.content() == FileContent.POSITION_DELETES) {
      posDeleteFileCount++;
      posDeleteFileSize += delete.fileSizeInBytes();
    } else {
      equalityDeleteFileCount++;
      equalityDeleteFileSize += delete.fileSizeInBytes();
    }
  }

  @Override
  public boolean isNecessary() {
    if (necessary == null) {
      if (isFullOptimizing()) {
        necessary = isFullNecessary();
      } else {
        necessary = isMajorNecessary() || isMinorNecessary();
      }
      LOG.debug("{} necessary = {}, {}", name(), necessary, this);
    }
    return necessary;
  }

  @Override
  public long getCost() {
    if (cost < 0) {
      // We estimate that the cost of writing is 3 times that of reading.
      // When rewriting the Position delete file, only the primary key field of the segment file
      // will be read, so only one-tenth of the size is calculated based on the size.
      cost =
          (fragmentFileSize + rewriteSegmentFileSize + undersizedSegmentFileSize) * 4 +
              rewritePosSegmentFileSize / 10 + posDeleteFileSize + equalityDeleteFileSize;
      int fileCnt =
          fragmentFileCount + rewriteSegmentFileCount + undersizedSegmentFileCount + rewritePosSegmentFileCount +
          posDeleteFileCount + equalityDeleteFileCount;
      cost += fileCnt * config.getOpenFileCost();
    }
    return cost;
  }

  @Override
  public PartitionEvaluator.Weight getWeight() {
    return new Weight(getCost());
  }

  @Override
  public OptimizingType getOptimizingType() {
    if (optimizingType == null) {
      optimizingType = isFullNecessary() ? OptimizingType.FULL : isMajorNecessary() ? OptimizingType.MAJOR :
          OptimizingType.MINOR;
      LOG.debug("{} optimizingType = {} ", name(), optimizingType);
    }
    return optimizingType;
  }

  /**
   * Segment files has enough content.
   *
   * <p>1. The total size of all undersized segment files is greater than target size
   *
   * <p>2. There are two undersized segment file that can be merged into one
   */
  public boolean enoughContent() {
    return undersizedSegmentFileSize >= config.getTargetSize() &&
        min1SegmentFileSize + min2SegmentFileSize <= config.getTargetSize();
  }

  public boolean isMajorNecessary() {
    return enoughContent() || rewriteSegmentFileCount > 0;
  }

  public boolean isMinorNecessary() {
    int smallFileCount = fragmentFileCount + equalityDeleteFileCount;
    return smallFileCount >= config.getMinorLeastFileCount() ||
        smallFileCount > 1 && reachMinorInterval() || combinePosSegmentFileCount > 0;
  }

  protected boolean reachMinorInterval() {
    return config.getMinorLeastInterval() >= 0 && planTime - tableRuntime.getLastMinorOptimizingTime() >
        config.getMinorLeastInterval();
  }

  protected boolean reachFullInterval() {
    return reachFullInterval;
  }

  public boolean isFullNecessary() {
    if (!reachFullInterval()) {
      return false;
    }
    return anyDeleteExist() || fragmentFileCount >= 2 || undersizedSegmentFileCount >= 2 ||
        rewriteSegmentFileCount > 0 || rewritePosSegmentFileCount > 0;
  }

  protected String name() {
    if (name == null) {
      name =
          String.format(
              "partition %s of %s", partition, tableRuntime.getTableIdentifier().toString());
    }
    return name;
  }

  public boolean anyDeleteExist() {
    return equalityDeleteFileCount > 0 || posDeleteFileCount > 0;
  }

  @Override
  public int getFragmentFileCount() {
    return fragmentFileCount;
  }

  @Override
  public long getFragmentFileSize() {
    return fragmentFileSize;
  }

  @Override
  public int getSegmentFileCount() {
    return rewriteSegmentFileCount + undersizedSegmentFileCount + rewritePosSegmentFileCount;
  }

  @Override
  public long getSegmentFileSize() {
    return rewriteSegmentFileSize + undersizedSegmentFileSize + rewritePosSegmentFileSize;
  }

  @Override
  public int getEqualityDeleteFileCount() {
    return equalityDeleteFileCount;
  }

  @Override
  public long getEqualityDeleteFileSize() {
    return equalityDeleteFileSize;
  }

  @Override
  public int getPosDeleteFileCount() {
    return posDeleteFileCount;
  }

  @Override
  public long getPosDeleteFileSize() {
    return posDeleteFileSize;
  }

  public static class Weight implements PartitionEvaluator.Weight {

    private final long cost;

    public Weight(long cost) {
      this.cost = cost;
    }

    @Override
    public int compareTo(PartitionEvaluator.Weight o) {
      return Long.compare(this.cost, ((Weight) o).cost);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partition", partition)
        .add("config", config)
        .add("fragmentSize", fragmentSize)
        .add("undersizedSegmentSize", minTargetSize)
        .add("planTime", planTime)
        .add("lastMinorOptimizeTime", tableRuntime.getLastMinorOptimizingTime())
        .add("lastFullOptimizeTime", tableRuntime.getLastFullOptimizingTime())
        .add("lastFullOptimizeTime", tableRuntime.getLastFullOptimizingTime())
        .add("fragmentFileCount", fragmentFileCount)
        .add("fragmentFileSize", fragmentFileSize)
        .add("rewriteSegmentFileCount", rewriteSegmentFileCount)
        .add("rewriteSegmentFileSize", rewriteSegmentFileSize)
        .add("undersizedSegmentFileCount", undersizedSegmentFileCount)
        .add("undersizedSegmentFileSize", undersizedSegmentFileSize)
        .add("rewritePosSegmentFileCount", rewritePosSegmentFileCount)
        .add("rewritePosSegmentFileSize", rewritePosSegmentFileSize)
        .add("min1SegmentFileSize", min1SegmentFileSize)
        .add("min2SegmentFileSize", min2SegmentFileSize)
        .add("equalityDeleteFileCount", equalityDeleteFileCount)
        .add("equalityDeleteFileSize", equalityDeleteFileSize)
        .add("posDeleteFileCount", posDeleteFileCount)
        .add("posDeleteFileSize", posDeleteFileSize)
        .toString();
  }
}
