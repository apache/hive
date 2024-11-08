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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.compaction.IcebergCompactionUtil;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.CommonPartitionEvaluator;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.IcebergTableFileScanHelper;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.OptimizingConfig;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableConfiguration;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableFileScanHelper;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableFormat;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableRuntime;
import org.apache.iceberg.mr.hive.compaction.evaluator.amoro.TableRuntimeMeta;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCompactionEvaluator {

  private static final long LAST_OPTIMIZE_TIME = 0;
  private static final int TRIGGER_INTERVAL = 0;

  private IcebergCompactionEvaluator() {

  }

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCompactionEvaluator.class);

  public static boolean isEligibleForCompaction(Table icebergTable, CompactionInfo ci, HiveConf conf) {

    if (icebergTable.currentSnapshot() == null) {
      LOG.info("Table {}{} doesn't require compaction because it is empty", icebergTable,
          ci.partName == null ? "" : " partition " + ci.partName);
      return false;
    }

    CommonPartitionEvaluator partitionEvaluator = createCommonPartitionEvaluator(icebergTable, ci, conf);

    if (partitionEvaluator == null) {
      return false;
    }

    switch (ci.type) {
      case MINOR:
        return partitionEvaluator.isMinorNecessary() || partitionEvaluator.isMajorNecessary();
      case MAJOR:
        return partitionEvaluator.isFullNecessary();
      default:
        return false;
    }
  }

  private static TableRuntime createTableRuntime(Table icebergTable, CompactionInfo ci, HiveConf conf) {
    OptimizingConfig optimizingConfig = OptimizingConfig.parse(Collections.emptyMap());
    optimizingConfig.setTargetSize(IcebergCompactionUtil.getTargetFileSize(ci, conf));
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

  private static CommonPartitionEvaluator createCommonPartitionEvaluator(Table table, CompactionInfo ci,
      HiveConf conf) {
    TableRuntime tableRuntime = createTableRuntime(table, ci, conf);
    long fileSizeThreshold = IcebergCompactionUtil.getFileSizeThreshold(ci, conf);

    TableFileScanHelper tableFileScanHelper = new IcebergTableFileScanHelper(table,
        table.currentSnapshot().snapshotId());
    CommonPartitionEvaluator evaluator = null;
    try (CloseableIterable<TableFileScanHelper.FileScanResult> results =
             tableFileScanHelper.scan()) {
      for (TableFileScanHelper.FileScanResult fileScanResult : results) {
        DataFile file = fileScanResult.file();
        if (IcebergCompactionUtil.shouldIncludeForCompaction(table, ci.partName, file, fileSizeThreshold)) {
          PartitionSpec partitionSpec = table.specs().get(file.specId());
          Pair<Integer, StructLike> partition = Pair.of(partitionSpec.specId(), fileScanResult.file().partition());

          if (evaluator == null) {
            evaluator = new CommonPartitionEvaluator(tableRuntime, partition, System.currentTimeMillis());
          }

          evaluator.addFile(fileScanResult.file(), fileScanResult.deleteFiles());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return evaluator;
  }
}
