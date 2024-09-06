/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.ThreadPools;

/**
 * Generic Mrv2 InputFormat API for Iceberg.
 *
 * @param <T> T is the in memory data model which can either be Pig tuples, Hive rows. Default is Iceberg records
 */
public class IcebergInputFormat<T> extends InputFormat<Void, T> {

  /**
   * Configures the {@code Job} to use the {@code IcebergInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   */
  public static InputFormatConfig.ConfigBuilder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return new InputFormatConfig.ConfigBuilder(job.getConfiguration());
  }

  private static TableScan createTableScan(Table table, Configuration conf) {
    TableScan scan = table.newScan();

    long snapshotId = -1;
    try {
      snapshotId = conf.getLong(InputFormatConfig.SNAPSHOT_ID, -1);
    } catch (NumberFormatException e) {
      String version = conf.get(InputFormatConfig.SNAPSHOT_ID);
      SnapshotRef ref = table.refs().get(version);
      if (ref == null) {
        throw new RuntimeException("Cannot find matching snapshot ID or reference name for version " + version);
      }
      snapshotId = ref.snapshotId();
    }
    String refName = conf.get(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF);
    if (StringUtils.isNotEmpty(refName)) {
      scan = scan.useRef(HiveUtils.getTableSnapshotRef(refName));
    }
    if (snapshotId != -1) {
      scan = scan.useSnapshot(snapshotId);
    }

    long asOfTime = conf.getLong(InputFormatConfig.AS_OF_TIMESTAMP, -1);
    if (asOfTime != -1) {
      scan = scan.asOfTime(asOfTime);
    }

    return scan;
  }

  private static IncrementalAppendScan createIncrementalAppendScan(Table table, Configuration conf) {
    long fromSnapshot = conf.getLong(InputFormatConfig.SNAPSHOT_ID_INTERVAL_FROM, -1);
    return table.newIncrementalAppendScan().fromSnapshotExclusive(fromSnapshot);
  }

  private static <
          T extends Scan<T, FileScanTask, CombinedScanTask>> Scan<T,
          FileScanTask,
          CombinedScanTask> applyConfig(
          Configuration conf, Scan<T, FileScanTask, CombinedScanTask> scanToConfigure) {

    Scan<T, FileScanTask, CombinedScanTask> scan = scanToConfigure.caseSensitive(
            conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT));

    long splitSize = conf.getLong(InputFormatConfig.SPLIT_SIZE, 0);
    if (splitSize > 0) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }

    // In case of LLAP-based execution we ask Iceberg not to combine multiple fileScanTasks into one split.
    // This is so that cache affinity can work, and each file(split) is executed/cached on always the same LLAP daemon.
    MapWork mapWork = LlapHiveUtils.findMapWork((JobConf) conf);
    if (mapWork != null && mapWork.getCacheAffinity()) {
      // Iceberg splits logically consist of buckets, where the bucket size equals to openFileCost setting if the files
      // assigned to such bucket are smaller. This is how Iceberg would combine multiple files into one split, so here
      // we need to enforce the bucket size to be equal to split size to avoid file combination.
      Long openFileCost = splitSize > 0 ? splitSize : TableProperties.SPLIT_SIZE_DEFAULT;
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(openFileCost));
    }
    //  TODO: Currently, this projection optimization stored on scan is not being used effectively on Hive side, as
    //   Hive actually uses conf to propagate the projected columns to let the final reader to read the only
    //   projected columns data. See IcebergInputFormat::readSchema(Configuration conf, Table table, boolean
    //   caseSensitive). But we can consider using this projection optimization stored on scan in the future when
    //   needed.
    Schema readSchema = InputFormatConfig.readSchema(conf);
    if (readSchema != null) {
      scan = scan.project(readSchema);
    } else {
      String[] selectedColumns = InputFormatConfig.selectedColumns(conf);
      if (selectedColumns != null) {
        scan = scan.select(selectedColumns);
      }
    }

    // TODO add a filter parser to get rid of Serialization
    Expression filter = SerializationUtil.deserializeFromBase64(conf.get(InputFormatConfig.FILTER_EXPRESSION));
    if (filter != null) {
      // In order to prevent the filter expression to be attached to every file scan task generated we call
      // ignoreResiduals() here. The passed in filter will still be effective during split generation.
      // On the execution side residual expressions will be mined from the passed job conf.
      scan = scan.filter(filter).ignoreResiduals();
    }
    return scan;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration conf = context.getConfiguration();
    Table table = Optional
        .ofNullable(HiveIcebergStorageHandler.table(conf, conf.get(InputFormatConfig.TABLE_IDENTIFIER)))
        .orElseGet(() -> {
          Table tbl = Catalogs.loadTable(conf);
          conf.set(InputFormatConfig.TABLE_IDENTIFIER, tbl.name());
          conf.set(InputFormatConfig.SERIALIZED_TABLE_PREFIX + tbl.name(), SerializationUtil.serializeToBase64(tbl));
          return tbl;
        });
    final ExecutorService workerPool =
        ThreadPools.newWorkerPool("iceberg-plan-worker-pool",
            conf.getInt(SystemConfigs.WORKER_THREAD_POOL_SIZE.propertyKey(), ThreadPools.WORKER_THREAD_POOL_SIZE));
    try {
      return planInputSplits(table, conf, workerPool);
    } finally {
      workerPool.shutdown();
    }
  }

  private List<InputSplit> planInputSplits(Table table, Configuration conf, ExecutorService workerPool) {
    List<InputSplit> splits = Lists.newArrayList();
    boolean applyResidual = !conf.getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);
    InputFormatConfig.InMemoryDataModel model = conf.getEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL,
        InputFormatConfig.InMemoryDataModel.GENERIC);

    long fromVersion = conf.getLong(InputFormatConfig.SNAPSHOT_ID_INTERVAL_FROM, -1);
    Scan<? extends Scan, FileScanTask, CombinedScanTask> scan;
    if (fromVersion != -1) {
      scan = applyConfig(conf, createIncrementalAppendScan(table, conf));
    } else {
      scan = applyConfig(conf, createTableScan(table, conf));
    }
    scan = scan.planWith(workerPool);

    boolean allowDataFilesWithinTableLocationOnly =
        conf.getBoolean(HiveConf.ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.varname,
            HiveConf.ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.defaultBoolVal);
    Path tableLocation = new Path(conf.get(InputFormatConfig.TABLE_LOCATION));


    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      tasksIterable.forEach(task -> {
        if (applyResidual && (model == InputFormatConfig.InMemoryDataModel.HIVE ||
            model == InputFormatConfig.InMemoryDataModel.PIG)) {
          // TODO: We do not support residual evaluation for HIVE and PIG in memory data model yet
          checkResiduals(task);
        }
        if (allowDataFilesWithinTableLocationOnly) {
          validateFileLocations(task, tableLocation);
        }
        splits.add(new IcebergSplit(conf, task));
      });
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to close table scan: %s", scan), e);
    }

    // If enabled, do not serialize FileIO hadoop config to decrease split size
    // However, do not skip serialization for metatable queries, because some metadata tasks cache the IO object and we
    // wouldn't be able to inject the config into these tasks on the deserializer-side, unlike for standard queries
    if (scan instanceof DataTableScan) {
      HiveIcebergStorageHandler.checkAndSkipIoConfigSerialization(conf, table);
    }

    return splits;
  }

  private static void validateFileLocations(CombinedScanTask split, Path tableLocation) {
    for (FileScanTask fileScanTask : split.files()) {
      if (!FileUtils.isPathWithinSubtree(new Path(fileScanTask.file().path().toString()), tableLocation)) {
        throw new AuthorizationException("The table contains paths which are outside the table location");
      }
    }
  }

  private static void checkResiduals(CombinedScanTask task) {
    task.files().forEach(fileScanTask -> {
      Expression residual = fileScanTask.residual();
      if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
        throw new UnsupportedOperationException(
            String.format(
                "Filter expression %s is not completely satisfied. Additional rows " +
                    "can be returned not satisfied by the filter expression", residual));
      }
    });
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return split instanceof IcebergMergeSplit ? new IcebergMergeRecordReader<>() : new IcebergRecordReader<>();
  }

}
