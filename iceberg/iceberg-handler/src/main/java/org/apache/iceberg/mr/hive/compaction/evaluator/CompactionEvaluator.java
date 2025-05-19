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

package org.apache.iceberg.mr.hive.compaction.evaluator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.mr.hive.compaction.IcebergCompactionUtil;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.CommonPartitionEvaluator;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.IcebergTableFileScanHelper;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.OptimizingConfig;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableConfiguration;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableFileScanHelper;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableFormat;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableProperties;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableRuntime;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableRuntimeMeta;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionEvaluator extends CommonPartitionEvaluator {

  private static final long LAST_OPTIMIZE_TIME = 0;
  private static final int TRIGGER_INTERVAL = -1;
  private final Table table;
  private final CompactionInfo ci;

  private static final Logger LOG = LoggerFactory.getLogger(CompactionEvaluator.class);

  public CompactionEvaluator(Table table, CompactionInfo ci, Map<String, String> parameters) throws IOException {
    super(
        createTableRuntime(table, parameters),
        getPartitionSpecStructPair(table, ci.partName),
        System.currentTimeMillis()
    );
    this.table = table;
    this.ci = ci;
    addFiles();
  }

  public boolean isEligibleForCompaction() {

    if (table.currentSnapshot() == null) {
      LOG.info("Table {}{} doesn't require compaction because it is empty", table,
          ci.partName == null ? "" : " partition " + ci.partName);
      return false;
    }

    addFiles();

    switch (ci.type) {
      case MINOR:
        return isMinorNecessary();
      case MAJOR:
        return isMajorNecessary();
      case SMART_OPTIMIZE:
        return isMinorNecessary() || isMajorNecessary();
      default:
        return false;
    }
  }

  public CompactionType determineCompactionType() {
    if (ci.type == CompactionType.SMART_OPTIMIZE) {
      if (isMajorNecessary()) {
        return CompactionType.MAJOR;
      } else if (isMinorNecessary()) {
        return CompactionType.MINOR;
      } else {
        return null;
      }
    } else {
      return ci.type;
    }
  }

  private static TableRuntime createTableRuntime(Table icebergTable, Map<String, String> parameters) {
    OptimizingConfig optimizingConfig = OptimizingConfig.parse(Collections.emptyMap());
    optimizingConfig.setTargetSize(getTargetSizeBytes(parameters));
    optimizingConfig.setFragmentRatio(getFragmentRatio(parameters));
    optimizingConfig.setMinTargetSizeRatio(getMinTargetSizeRatio(parameters));
    optimizingConfig.setMinorLeastFileCount(getMinInputFiles(parameters));
    optimizingConfig.setMajorDuplicateRatio(getDeleteFileRatio(parameters));
    optimizingConfig.setFullTriggerInterval(TRIGGER_INTERVAL);
    optimizingConfig.setMinorLeastInterval(TRIGGER_INTERVAL);

    TableConfiguration tableConfig = new TableConfiguration();
    tableConfig.setOptimizingConfig(optimizingConfig);

    TableRuntimeMeta tableRuntimeMeta = new TableRuntimeMeta();
    tableRuntimeMeta.setTableName(icebergTable.name());
    tableRuntimeMeta.setFormat(TableFormat.ICEBERG);
    tableRuntimeMeta.setLastFullOptimizingTime(LAST_OPTIMIZE_TIME);
    tableRuntimeMeta.setLastMinorOptimizingTime(LAST_OPTIMIZE_TIME);
    tableRuntimeMeta.setTableConfig(tableConfig);

    return new HiveTableRuntime(tableRuntimeMeta);
  }

  private void addFiles() {
    TableFileScanHelper tableFileScanHelper = new IcebergTableFileScanHelper(table,
        table.currentSnapshot().snapshotId());
    try (CloseableIterable<TableFileScanHelper.FileScanResult> results =
             tableFileScanHelper.scan()) {
      for (TableFileScanHelper.FileScanResult fileScanResult : results) {
        DataFile file = fileScanResult.file();
        if (IcebergCompactionUtil.shouldIncludeForCompaction(table, ci.partName, file)) {
          addFile(fileScanResult.file(), fileScanResult.deleteFiles());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static long getTargetSizeBytes(Map<String, String> parameters) {
    return Optional.ofNullable(parameters.get(CompactorContext.COMPACTION_TARGET_SIZE))
        .map(Long::parseLong)
        .orElse(TableProperties.SELF_OPTIMIZING_TARGET_SIZE_DEFAULT);
  }

  public static double getMinTargetSizeRatio(Map<String, String> parameters) {
    return Optional.ofNullable(parameters.get(CompactorContext.COMPACTION_MIN_TARGET_SIZE_RATIO))
        .map(Double::parseDouble)
        .orElse(TableProperties.SELF_OPTIMIZING_MIN_TARGET_SIZE_RATIO_DEFAULT);
  }

  public static int getFragmentRatio(Map<String, String> parameters) {
    return Optional.ofNullable(parameters.get(CompactorContext.COMPACTION_MIN_FRAGMENT_RATIO))
        .map(x -> (int) (1 / Double.parseDouble(x)))
        .orElse(TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO_DEFAULT);
  }

  public static int getFragmentSizeBytes(Map<String, String> parameters) {
    return (int) (getTargetSizeBytes(parameters) * getMinTargetSizeRatio(parameters));
  }

  public static int getMinInputFiles(Map<String, String> parameters) {
    return Optional.ofNullable(parameters.get(CompactorContext.COMPACTION_MIN_INPUT_FILES))
        .map(Integer::parseInt)
        .orElse(TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT_DEFAULT);
  }

  public static double getDeleteFileRatio(Map<String, String> parameters) {
    return Optional.ofNullable(parameters.get(CompactorContext.COMPACTION_DELETE_FILE_RATIO))
        .map(Double::parseDouble)
        .orElse(TableProperties.SELF_OPTIMIZING_MAJOR_TRIGGER_DUPLICATE_RATIO_DEFAULT);
  }

  private static Pair<Integer, StructLike> getPartitionSpecStructPair(Table table, String partitionPath)
      throws IOException {
    if (!table.spec().isPartitioned() || partitionPath == null) {
      return null;
    }
    PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils
        .createMetadataTableInstance(table, MetadataTableType.PARTITIONS);
    try (CloseableIterable<FileScanTask> fileScanTasks = partitionsTable.newScan().planFiles()) {
      return FluentIterable.from(fileScanTasks)
          .transformAndConcat(task -> task.asDataTask().rows())
          .transform(row -> {
            StructLike data = row.get(IcebergTableUtil.PART_IDX, StructProjection.class);
            PartitionSpec spec = table.specs().get(row.get(IcebergTableUtil.SPEC_IDX, Integer.class));
            PartitionData partitionData = IcebergTableUtil.toPartitionData(data,
                Partitioning.partitionType(table), spec.partitionType());
            String path = spec.partitionToPath(partitionData);
            return Maps.immutableEntry(path, Pair.of(spec.specId(), data));
          })
          .filter(e -> e.getKey().equals(partitionPath))
          .transform(Map.Entry::getValue)
          .get(0);
    }
  }
}
