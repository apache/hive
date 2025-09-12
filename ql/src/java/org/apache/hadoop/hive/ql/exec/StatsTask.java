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

package org.apache.hadoop.hive.ql.exec;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.stats.BasicStatsNoJobTask;
import org.apache.hadoop.hive.ql.stats.BasicStatsTask;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor;
import org.apache.hadoop.hive.ql.stats.IStatsProcessor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * StatsTask implementation.
 **/

public class StatsTask extends Task<StatsWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(StatsTask.class);
  private final PerfLogger perfLogger = SessionState.getPerfLogger();

  public StatsTask() {
    super();
  }

  List<IStatsProcessor> processors = new ArrayList<>();

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
    super.initialize(queryState, queryPlan, taskQueue, context);

    if (work.getBasicStatsWork() != null) {
      BasicStatsTask task = new BasicStatsTask(conf, work.getBasicStatsWork());
      task.followedColStats = work.hasColStats();
      processors.add(0, task);
    } else if (work.isFooterScan()) {
      BasicStatsNoJobTask t = new BasicStatsNoJobTask(conf, work.getBasicStatsNoJobWork());
      processors.add(0, t);
    }
    if (work.hasColStats()) {
      processors.add(new ColStatsProcessor(work.getColStats(), conf));
    }

    for (IStatsProcessor p : processors) {
      p.initialize(context.getOpContext());
    }
  }


  @Override
  public int execute() {
    if (context.getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }
    if (work.isAggregating() && work.isFooterScan()) {
      throw new RuntimeException("Can not have both basic stats work and stats no job work!");
    }
    int ret = 0;
    try {
      perfLogger.perfLogBegin("StatsTask", PerfLogger.STATS_TASK);

      if (work.isFooterScan()) {
        work.getBasicStatsNoJobWork().setPartitions(work.getPartitions());
      }

      Hive db = getHive();
      Table tbl = getTable(db);

      for (IStatsProcessor task : processors) {
        task.setDpPartSpecs(dpPartSpecs);
        ret = task.process(db, tbl);
        if (ret != 0) {
          return ret;
        }
      }

      // ADD: Check for small files after successful stats computation
      if (ret == 0) {
        checkSmallFilesAfterAnalyze(db, tbl);
      }

    } catch (Exception e) {
      LOG.error("Failed to run stats task", e);
      setException(e);
      return 1;
    } finally {
      perfLogger.perfLogEnd("StatsTask", PerfLogger.STATS_TASK);
      console.printInfo(String.format("StatsTask took %d", perfLogger.getDuration(PerfLogger.STATS_TASK)));
    }
    return 0;
  }

  /**
   * Check for small files after ANALYZE completes
   */
  private void checkSmallFilesAfterAnalyze(Hive db, Table tbl) {
    // Use the existing merge small files threshold configuration
    long smallFilesThreshold = conf.getLongVar(HiveConf.ConfVars.HIVE_MERGE_MAP_FILES_AVG_SIZE);
    List<String> smallFilesWarnings = new ArrayList<>();

    // Get the console from SessionState for output
    PrintStream console = SessionState.get().out;

    try {
      if (tbl.isPartitioned()) {
        // For partitioned tables
        checkPartitionedTableSmallFiles(db, tbl, smallFilesThreshold, smallFilesWarnings);
      } else {
        // For non-partitioned tables
        checkNonPartitionedTableSmallFiles(tbl, smallFilesThreshold, smallFilesWarnings);
      }

      // Output warnings to console
      reportSmallFilesWarnings(tbl.getFullyQualifiedName(),
              smallFilesWarnings, smallFilesThreshold, console);

    } catch (Exception e) {
      LOG.warn("Failed to check for small files after ANALYZE", e);
      // Don't fail the ANALYZE task due to small files check failure
    }
  }

  private void checkPartitionedTableSmallFiles(Hive db, Table tbl, long threshold,
                                               List<String> smallFilesWarnings) throws HiveException {
    // Check partitions that were analyzed
    List<Partition> partitionsToCheck = null;

    if (work.getPartitions() != null && !work.getPartitions().isEmpty()) {
      // Specific partitions were analyzed
      partitionsToCheck = new ArrayList<>(work.getPartitions());
    } else if (dpPartSpecs != null && !dpPartSpecs.isEmpty()) {
      // Dynamic partitions
      partitionsToCheck = new ArrayList<>(dpPartSpecs);
    } else {
      // All partitions - get from metastore
      partitionsToCheck = db.getPartitions(tbl);
    }

    if (partitionsToCheck == null || partitionsToCheck.isEmpty()) {
      return;
    }

    for (Partition partition : partitionsToCheck) {
      Map<String, String> params = partition.getParameters();
      checkPartitionStats(partition.getName(), params, threshold, smallFilesWarnings);
    }
  }

  private void checkNonPartitionedTableSmallFiles(Table tbl, long threshold,
                                                  List<String> smallFilesWarnings) {
    Map<String, String> params = tbl.getParameters();
    checkPartitionStats(tbl.getTableName(), params, threshold, smallFilesWarnings);
  }

  private void checkPartitionStats(String name, Map<String, String> params, long threshold,
                                   List<String> smallFilesWarnings) {
    String numFilesStr = params.get(StatsSetupConst.NUM_FILES);
    String totalSizeStr = params.get(StatsSetupConst.TOTAL_SIZE);

    // Skip if statistics are not available
    if (numFilesStr == null || totalSizeStr == null ||
            "0".equals(numFilesStr) || "0".equals(totalSizeStr)) {
      return;
    }

    try {
      long numFiles = Long.parseLong(numFilesStr);
      long totalSize = Long.parseLong(totalSizeStr);

      if (numFiles > 0) {
        long avgSize = totalSize / numFiles;
        if (avgSize < threshold) {
          smallFilesWarnings.add(String.format("  - %s (avg: %s, files: %d)",
                  name,
                  humanReadableSize(avgSize),
                  numFiles));
        }
      }
    } catch (NumberFormatException e) {
      LOG.warn("Invalid stats for {}: numFiles={}, totalSize={}",
              name, numFilesStr, totalSizeStr);
    }
  }

  private void reportSmallFilesWarnings(String tableName,
                                        List<String> smallFilesWarnings,
                                        long threshold,
                                        PrintStream console) {
    if (!smallFilesWarnings.isEmpty()) {
      console.println();
      console.println("WARNING: Small files detected after ANALYZE:");
      console.println("Table: " + tableName);
      console.println("Threshold: " + humanReadableSize(threshold));
      smallFilesWarnings.forEach(console::println);
      console.println("Consider using 'ALTER TABLE ... CONCATENATE' to merge small files.");
      console.println();

      // Also log the warning
      LOG.warn("Small files detected in table {} with {} partitions/locations having small files",
              tableName, smallFilesWarnings.size());
    }
  }

  // convert the size in bytes into more readable information
  private String humanReadableSize(long bytes) {
    if (bytes < 1024) return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(1024));
    String pre = "KMGTPE".charAt(exp - 1) + "";
    return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
  }


  private Table getTable(Hive db) throws SemanticException, HiveException {
    return db.getTable(work.getFullTableName());
  }

  @Override
  public StageType getType() {
    return StageType.STATS;
  }

  @Override
  public String getName() {
    return "STATS TASK";
  }

  private Collection<Partition> dpPartSpecs;

  @Override
  protected void receiveFeed(FeedType feedType, Object feedValue) {
    // this method should be called by MoveTask when there are dynamic
    // partitions generated
    if (feedType == FeedType.DYNAMIC_PARTITIONS) {
      dpPartSpecs = (Collection<Partition>) feedValue;
    }
  }

  public static ExecutorService newThreadPool(HiveConf conf) {
    int numThreads = HiveConf.getIntVar(conf, ConfVars.HIVE_STATS_GATHER_NUM_THREADS);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("StatsNoJobTask-Thread-%d").build());
    LOG.info("Initialized threadpool for stats computation with {} threads", numThreads);
    return executor;
  }
}
