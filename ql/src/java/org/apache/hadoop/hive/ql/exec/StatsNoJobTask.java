/**
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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.plan.StatsNoJobWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * StatsNoJobTask is used in cases where stats collection is the only task for the given query (no
 * parent MR or Tez job). It is used in the following cases 1) ANALYZE with partialscan/noscan for
 * file formats that implement StatsProvidingRecordReader interface: ORC format (implements
 * StatsProvidingRecordReader) stores column statistics for all columns in the file footer. Its much
 * faster to compute the table/partition statistics by reading the footer than scanning all the
 * rows. This task can be used for computing basic stats like numFiles, numRows, fileSize,
 * rawDataSize from ORC footer.
 **/
public class StatsNoJobTask extends Task<StatsNoJobWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static transient final Log LOG = LogFactory.getLog(StatsNoJobTask.class);
  private static ConcurrentMap<String, Partition> partUpdates;
  private static Table table;
  private static String tableFullName;
  private static JobConf jc = null;

  public StatsNoJobTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);
    jc = new JobConf(conf);
  }

  @Override
  public int execute(DriverContext driverContext) {

    LOG.info("Executing stats (no job) task");

    String tableName = "";
    ExecutorService threadPool = null;
    try {
      tableName = work.getTableSpecs().tableName;
      table = db.getTable(tableName);
      int numThreads = HiveConf.getIntVar(conf, ConfVars.HIVE_STATS_GATHER_NUM_THREADS);
      tableFullName = table.getDbName() + "." + table.getTableName();
      threadPool = Executors.newFixedThreadPool(numThreads,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("StatsNoJobTask-Thread-%d")
              .build());
      partUpdates = new MapMaker().concurrencyLevel(numThreads).makeMap();
      LOG.info("Initialized threadpool for stats computation with " + numThreads + " threads");
    } catch (HiveException e) {
      LOG.error("Cannot get table " + tableName, e);
      console.printError("Cannot get table " + tableName, e.toString());
    }

    return aggregateStats(threadPool);
  }

  @Override
  public StageType getType() {
    return StageType.STATS;
  }

  @Override
  public String getName() {
    return "STATS-NO-JOB";
  }

  class StatsCollection implements Runnable {

    private Partition partn;

    public StatsCollection(Partition part) {
      this.partn = part;
    }

    @Override
    public void run() {

      // get the list of partitions
      org.apache.hadoop.hive.metastore.api.Partition tPart = partn.getTPartition();
      Map<String, String> parameters = tPart.getParameters();

      try {
        Path dir = new Path(tPart.getSd().getLocation());
        long numRows = 0;
        long rawDataSize = 0;
        long fileSize = 0;
        long numFiles = 0;
        FileSystem fs = dir.getFileSystem(conf);
        List<FileStatus> fileList = 
          ShimLoader.getHadoopShims().listLocatedStatus(fs, dir,
                                                        hiddenFileFilter);
        boolean statsAvailable = false;
        for(FileStatus file: fileList) {
          if (!file.isDir()) {
            InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) ReflectionUtils.newInstance(
                partn.getInputFormatClass(), jc);
            InputSplit dummySplit = new FileSplit(file.getPath(), 0, 0,
                new String[] { partn.getLocation() });
            org.apache.hadoop.mapred.RecordReader<?, ?> recordReader =
                (org.apache.hadoop.mapred.RecordReader<?, ?>)
                inputFormat.getRecordReader(dummySplit, jc, Reporter.NULL);
            StatsProvidingRecordReader statsRR;
            if (recordReader instanceof StatsProvidingRecordReader) {
              statsRR = (StatsProvidingRecordReader) recordReader;
              rawDataSize += statsRR.getStats().getRawDataSize();
              numRows += statsRR.getStats().getRowCount();
              fileSize += file.getLen();
              numFiles += 1;
              statsAvailable = true;
            }
            recordReader.close();
          }
        }

        if (statsAvailable) {
          parameters.put(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
          parameters.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(rawDataSize));
          parameters.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(fileSize));
          parameters.put(StatsSetupConst.NUM_FILES, String.valueOf(numFiles));
          parameters.put(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, StatsSetupConst.TRUE);

          partUpdates.put(tPart.getSd().getLocation(), new Partition(table, tPart));

          // printout console and debug logs
          String threadName = Thread.currentThread().getName();
          String msg = "Partition " + tableFullName + partn.getSpec() + " stats: ["
              + toString(parameters) + ']';
          LOG.debug(threadName + ": " + msg);
          console.printInfo(msg);
        } else {
          String threadName = Thread.currentThread().getName();
          String msg = "Partition " + tableFullName + partn.getSpec() + " does not provide stats.";
          LOG.debug(threadName + ": " + msg);
        }
      } catch (Exception e) {
        console.printInfo("[Warning] could not update stats for " + tableFullName + partn.getSpec()
            + ".",
            "Failed with exception " + e.getMessage() + "\n" + StringUtils.stringifyException(e));

        // Before updating the partition params, if any partition params is null
        // and if statsReliable is true then updatePartition() function  will fail 
        // the task by returning 1
        if (work.isStatsReliable()) {
          partUpdates.put(tPart.getSd().getLocation(), null);
        }
      }
    }

    private String toString(Map<String, String> parameters) {
      StringBuilder builder = new StringBuilder();
      for (String statType : StatsSetupConst.supportedStats) {
        String value = parameters.get(statType);
        if (value != null) {
          if (builder.length() > 0) {
            builder.append(", ");
          }
          builder.append(statType).append('=').append(value);
        }
      }
      return builder.toString();
    }

  }

  private int aggregateStats(ExecutorService threadPool) {
    int ret = 0;

    try {
      List<Partition> partitions = getPartitionsList();

      // non-partitioned table
      if (partitions == null) {
        org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
        Map<String, String> parameters = tTable.getParameters();
        try {
          Path dir = new Path(tTable.getSd().getLocation());
          long numRows = 0;
          long rawDataSize = 0;
          long fileSize = 0;
          long numFiles = 0;
          FileSystem fs = dir.getFileSystem(conf);
          List<FileStatus> fileList = 
            ShimLoader.getHadoopShims().listLocatedStatus(fs, dir,
                                                          hiddenFileFilter);
          boolean statsAvailable = false;
          for(FileStatus file: fileList) {
            if (!file.isDir()) {
              InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) ReflectionUtils.newInstance(
                  table.getInputFormatClass(), jc);
              InputSplit dummySplit = new FileSplit(file.getPath(), 0, 0, new String[] { table
                  .getDataLocation().toString() });
              org.apache.hadoop.mapred.RecordReader<?, ?> recordReader = (org.apache.hadoop.mapred.RecordReader<?, ?>) inputFormat
                  .getRecordReader(dummySplit, jc, Reporter.NULL);
              StatsProvidingRecordReader statsRR;
              if (recordReader instanceof StatsProvidingRecordReader) {
                statsRR = (StatsProvidingRecordReader) recordReader;
                numRows += statsRR.getStats().getRowCount();
                rawDataSize += statsRR.getStats().getRawDataSize();
                fileSize += file.getLen();
                numFiles += 1;
                statsAvailable = true;
              }
              recordReader.close();
            }
          }

          if (statsAvailable) {
            parameters.put(StatsSetupConst.ROW_COUNT, String.valueOf(numRows));
            parameters.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(rawDataSize));
            parameters.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(fileSize));
            parameters.put(StatsSetupConst.NUM_FILES, String.valueOf(numFiles));
            parameters.put(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, StatsSetupConst.TRUE);

            db.alterTable(tableFullName, new Table(tTable));

            String msg = "Table " + tableFullName + " stats: [" + toString(parameters) + ']';
            LOG.debug(msg);
            console.printInfo(msg);
          } else {
            String msg = "Table " + tableFullName + " does not provide stats.";
            LOG.debug(msg);
          }
        } catch (Exception e) {
          console.printInfo("[Warning] could not update stats for " + tableFullName + ".",
              "Failed with exception " + e.getMessage() + "\n" + StringUtils.stringifyException(e));
        }
      } else {

        // Partitioned table
        for (Partition partn : partitions) {
          threadPool.execute(new StatsCollection(partn));
        }

        LOG.debug("Stats collection waiting for threadpool to shutdown..");
        shutdownAndAwaitTermination(threadPool);
        LOG.debug("Stats collection threadpool shutdown successful.");

        ret = updatePartitions();
      }

    } catch (Exception e) {
      // Fail the query if the stats are supposed to be reliable
      if (work.isStatsReliable()) {
        ret = -1;
      }
    }

    // The return value of 0 indicates success,
    // anything else indicates failure
    return ret;
  }

  private int updatePartitions() throws InvalidOperationException, HiveException {
    if (!partUpdates.isEmpty()) {
      List<Partition> updatedParts = Lists.newArrayList(partUpdates.values());
      if (updatedParts.contains(null) && work.isStatsReliable()) {
        LOG.debug("Stats requested to be reliable. Empty stats found and hence failing the task.");
        return -1;
      } else {
        LOG.debug("Bulk updating partitions..");
        db.alterPartitions(tableFullName, Lists.newArrayList(partUpdates.values()));
        LOG.debug("Bulk updated " + partUpdates.values().size() + " partitions.");
      }
    }
    return 0;
  }

  private void shutdownAndAwaitTermination(ExecutorService threadPool) {

    // Disable new tasks from being submitted
    threadPool.shutdown();
    try {

      // Wait a while for existing tasks to terminate
      if (!threadPool.awaitTermination(100, TimeUnit.SECONDS)) {
        // Cancel currently executing tasks
        threadPool.shutdownNow();

        // Wait a while for tasks to respond to being cancelled
        if (!threadPool.awaitTermination(100, TimeUnit.SECONDS)) {
          LOG.debug("Stats collection thread pool did not terminate");
        }
      }
    } catch (InterruptedException ie) {

      // Cancel again if current thread also interrupted
      threadPool.shutdownNow();

      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private String toString(Map<String, String> parameters) {
    StringBuilder builder = new StringBuilder();
    for (String statType : StatsSetupConst.supportedStats) {
      String value = parameters.get(statType);
      if (value != null) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append(statType).append('=').append(value);
      }
    }
    return builder.toString();
  }

  private List<Partition> getPartitionsList() throws HiveException {
    if (work.getTableSpecs() != null) {
      tableSpec tblSpec = work.getTableSpecs();
      table = tblSpec.tableHandle;
      if (!table.isPartitioned()) {
        return null;
      } else {
        return tblSpec.partitions;
      }
    }
    return null;
  }
}
