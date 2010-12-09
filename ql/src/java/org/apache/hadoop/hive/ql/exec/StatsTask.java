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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;
import org.apache.hadoop.util.StringUtils;

/**
 * StatsTask implementation.
 **/
public class StatsTask extends Task<StatsWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  private Table table;
  private List<LinkedHashMap<String, String>> dpPartSpecs;

  public StatsTask() {
    super();
    dpPartSpecs = null;
  }

  /**
   *
   * Partition Level Statistics.
   *
   */
  class PartitionStatistics {
    int numFiles; // number of files in the partition
    long numRows;  // number of rows in the partition
    long size;    // total size in bytes of the partition

    public PartitionStatistics() {
      numFiles = 0;
      numRows = 0L;
      size = 0L;
    }

    public PartitionStatistics(int nf, long nr, long sz) {
      numFiles = nf;
      numRows = nr;
      size = sz;
    }

    public int getNumFiles() {
      return numFiles;
    }

    public long getNumRows() {
      return numRows;
    }

    public long getSize() {
      return size;
    }

    public void setNumFiles(int nf) {
      numFiles = nf;
    }

    public void setNumRows(long nr) {
      numRows = nr;
    }

    public void setSize(long sz) {
      size = sz;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("num_files: ").append(numFiles).append(", ");
      sb.append("num_rows: ").append(numRows).append(", ");
      sb.append("total_size: ").append(size);
      return sb.toString();
    }
  }

  /**
   *
   * Table Level Statistics.
   *
   */
  class TableStatistics extends PartitionStatistics {
    int numPartitions; // number of partitions

    public TableStatistics() {
      super();
      numPartitions = 0;
    }

    public void setNumPartitions(int np) {
      numPartitions = np;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    /**
     * Incrementally update the table statistics according to the old and new
     * partition level statistics.
     * @param oldStats The old statistics of a partition.
     * @param newStats The new statistics of a partition.
     */
    public void updateStats(PartitionStatistics oldStats, PartitionStatistics newStats) {
      deletePartitionStats(oldStats);
      addPartitionStats(newStats);
    }

    /**
     * Update the table level statistics when a new partition is added.
     * @param newStats the new partition statistics.
     */
    public void addPartitionStats(PartitionStatistics newStats) {
      this.numFiles += newStats.getNumFiles();
      this.numRows += newStats.getNumRows();
      this.size += newStats.getSize();
      this.numPartitions++;
    }

    /**
     * Update the table level statistics when an old partition is dropped.
     * @param oldStats the old partition statistics.
     */
    public void deletePartitionStats(PartitionStatistics oldStats) {
      this.numFiles -= oldStats.getNumFiles();
      this.numRows -= oldStats.getNumRows();
      this.size -= oldStats.getSize();
      this.numPartitions--;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("num_partitions: ").append(numPartitions).append(", ");
      sb.append(super.toString());
      return sb.toString();
    }
  }

  @Override
  protected void receiveFeed(FeedType feedType, Object feedValue) {
    // this method should be called by MoveTask when there are dynamic partitions generated
    if (feedType == FeedType.DYNAMIC_PARTITIONS) {
      assert feedValue instanceof List<?>;
      dpPartSpecs = (List<LinkedHashMap<String, String>>) feedValue;
    }
  }

  @Override
  public int execute(DriverContext driverContext) {

    // Make sure that it is either an ANALYZE command or an INSERT OVERWRITE command
    assert (work.getLoadTableDesc() != null && work.getTableSpecs() == null ||
            work.getLoadTableDesc() == null && work.getTableSpecs() != null);
    String tableName = "";
    try {
      if (work.getLoadTableDesc() != null) {
        tableName = work.getLoadTableDesc().getTable().getTableName();
      } else {
        tableName = work.getTableSpecs().tableName;
      }
      table = db.getTable(tableName);
    }  catch (HiveException e) {
       LOG.error("Cannot get table " + tableName, e);
       console.printError("Cannot get table " + tableName, e.toString());
    }
    return aggregateStats();
  }

  @Override
  public StageType getType() {
    return StageType.STATS;
  }

  @Override
  public String getName() {
    return "STATS";
  }

  @Override
  protected void localizeMRTmpFilesImpl(Context ctx) {
    // Nothing to do for StatsTask here.
  }

  private int aggregateStats() {
    try {
      // Stats setup:
      Warehouse wh = new Warehouse(conf);
      FileSystem fileSys;
      FileStatus[] fileStatus;

      // manufacture a StatsAggregator
      StatsAggregator statsAggregator;
      String statsImplementationClass = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
      StatsFactory.setImplementation(statsImplementationClass, conf);
      statsAggregator = StatsFactory.getStatsAggregator();
      if (!statsAggregator.connect(conf)) {
        // this should not fail the whole job, return 0 so that the job won't fail.
        console.printInfo("[WARNING] Could not update table/partition level stats.",
            "StatsAggregator.connect() failed: stats class = " +
            statsImplementationClass);
        return 0;
      }


      TableStatistics tblStats = new TableStatistics();

      //
      // For partitioned table get the old table statistics for incremental update
      //
      if (table.isPartitioned()) {
        org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
        Map<String, String> parameters = tTable.getParameters();
        if (parameters.containsKey(StatsSetupConst.ROW_COUNT)) {
          tblStats.setNumRows(Long.parseLong(parameters.get(StatsSetupConst.ROW_COUNT)));
        }
        if (parameters.containsKey(StatsSetupConst.NUM_PARTITIONS)) {
          tblStats.setNumPartitions(Integer.parseInt(parameters.get(StatsSetupConst.NUM_PARTITIONS)));
        }
        if (parameters.containsKey(StatsSetupConst.NUM_FILES)) {
          tblStats.setNumFiles(Integer.parseInt(parameters.get(StatsSetupConst.NUM_FILES)));
        }
        if (parameters.containsKey(StatsSetupConst.TOTAL_SIZE)) {
          tblStats.setSize(Long.parseLong(parameters.get(StatsSetupConst.TOTAL_SIZE)));
        }
      }

      List<Partition> partitions = getPartitionsList();

      if (partitions == null) {
        // non-partitioned tables:

        Path tablePath = wh.getDefaultTablePath(table.getDbName(), table.getTableName());
        fileSys = tablePath.getFileSystem(conf);
        fileStatus = Utilities.getFileStatusRecurse(tablePath, 1, fileSys);
        tblStats.setNumFiles(fileStatus.length);
        long tableSize = 0L;
        for (int i = 0; i < fileStatus.length; i++) {
          tableSize += fileStatus[i].getLen();
        }
        tblStats.setSize(tableSize);

        // In case of a non-partitioned table, the key for stats temporary store is "rootDir"
        String rows = statsAggregator.aggregateStats(work.getAggKey(), StatsSetupConst.ROW_COUNT);
        if (rows != null) {
          tblStats.setNumRows(Long.parseLong(rows));
        }
      } else {
        // Partitioned table:
        // Need to get the old stats of the partition
        // and update the table stats based on the old and new stats.
        for (Partition partn : partitions) {
          //
          // get the new partition stats
          //
          PartitionStatistics newPartStats = new PartitionStatistics();

          // In that case of a partition, the key for stats temporary store is "rootDir/[dynamic_partition_specs/]%"
          String partitionID = work.getAggKey() + Warehouse.makePartPath(partn.getSpec());

          String rows = statsAggregator.aggregateStats(partitionID, StatsSetupConst.ROW_COUNT);
          if (rows != null) {
            newPartStats.setNumRows(Long.parseLong(rows));
          }

          fileSys = partn.getPartitionPath().getFileSystem(conf);
          fileStatus = Utilities.getFileStatusRecurse(partn.getPartitionPath(), 1, fileSys);
          newPartStats.setNumFiles(fileStatus.length);

          long partitionSize = 0L;
          for (int i = 0; i < fileStatus.length; i++) {
            partitionSize += fileStatus[i].getLen();
          }
          newPartStats.setSize(partitionSize);

          //
          // get the old partition stats
          //
          org.apache.hadoop.hive.metastore.api.Partition tPart = partn.getTPartition();
          Map<String, String> parameters = tPart.getParameters();

          boolean hasStats =
            parameters.containsKey(StatsSetupConst.NUM_FILES) ||
            parameters.containsKey(StatsSetupConst.ROW_COUNT) ||
            parameters.containsKey(StatsSetupConst.TOTAL_SIZE);

          int  nf = parameters.containsKey(StatsSetupConst.NUM_FILES) ?
                    Integer.parseInt(parameters.get(StatsSetupConst.NUM_FILES)) :
                    0;
          long nr = parameters.containsKey(StatsSetupConst.ROW_COUNT) ?
                    Long.parseLong(parameters.get(StatsSetupConst.ROW_COUNT)) :
                    0L;
          long sz = parameters.containsKey(StatsSetupConst.TOTAL_SIZE) ?
                    Long.parseLong(parameters.get(StatsSetupConst.TOTAL_SIZE)) :
                    0L;
          if (hasStats) {
            PartitionStatistics oldPartStats = new PartitionStatistics(nf, nr, sz);
            tblStats.updateStats(oldPartStats, newPartStats);
          } else {
            tblStats.addPartitionStats(newPartStats);
          }

          //
          // update the metastore
          //
          parameters.put(StatsSetupConst.ROW_COUNT, Long.toString(newPartStats.getNumRows()));
          parameters.put(StatsSetupConst.NUM_FILES, Integer.toString(newPartStats.getNumFiles()));
          parameters.put(StatsSetupConst.TOTAL_SIZE, Long.toString(newPartStats.getSize()));

          tPart.setParameters(parameters);
          db.alterPartition(table.getTableName(), new Partition(table, tPart));

          console.printInfo("Partition " + table.getTableName() + partn.getSpec() +
              " stats: [" + newPartStats.toString() + ']');
        }
      }

      statsAggregator.closeConnection();

      //
      // write table stats to metastore
      //
      org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
      Map<String, String> parameters = tTable.getParameters();
      parameters.put(StatsSetupConst.ROW_COUNT, Long.toString(tblStats.getNumRows()));
      parameters.put(StatsSetupConst.NUM_PARTITIONS, Integer.toString(tblStats.getNumPartitions()));
      parameters.put(StatsSetupConst.NUM_FILES, Integer.toString(tblStats.getNumFiles()));
      parameters.put(StatsSetupConst.TOTAL_SIZE, Long.toString(tblStats.getSize()));
      tTable.setParameters(parameters);

      db.alterTable(table.getTableName(), new Table(tTable));

      console.printInfo("Table " + table.getTableName() + " stats: [" + tblStats.toString() + ']');

      return 0;
    }
    catch (Exception e) {
      // return 0 since StatsTask should not fail the whole job
      console.printInfo("[Warning] could not update stats.",
          "Failed with exception " + e.getMessage() + "\n"
          + StringUtils.stringifyException(e));
      return 0;
    }
  }

  /**
   * Get the list of partitions that need to update statistics.
   * TODO: we should reuse the Partitions generated at compile time
   * since getting the list of partitions is quite expensive.
   * @return a list of partitions that need to update statistics.
   * @throws HiveException
   */
  private List<Partition> getPartitionsList() throws HiveException {

    List<Partition> list = new ArrayList<Partition>();

    if (work.getTableSpecs() != null) {

      // ANALYZE command
      tableSpec tblSpec = work.getTableSpecs();
      table = tblSpec.tableHandle;
      if (!table.isPartitioned()) {
        return null;
      }
      // get all partitions that matches with the partition spec
      List<Partition> partitions = tblSpec.partitions;
      if (partitions != null) {
        for (Partition partn : partitions) {
          list.add(partn);
        }
      }
    } else if (work.getLoadTableDesc() != null) {

      // INSERT OVERWRITE command
      LoadTableDesc tbd = work.getLoadTableDesc();
      table = db.getTable(tbd.getTable().getTableName());
      if (!table.isPartitioned()) {
        return null;
      }
      DynamicPartitionCtx dpCtx = tbd.getDPCtx();
      if (dpCtx != null && dpCtx.getNumDPCols() > 0) { // dynamic partitions
        // load the list of DP partitions and return the list of partition specs
        for (LinkedHashMap<String, String> partSpec: dpPartSpecs) {
          Partition partn = db.getPartition(table, partSpec, false);
          list.add(partn);
        }
      } else { // static partition
        Partition partn = db.getPartition(table, tbd.getPartitionSpec(), false);
        list.add(partn);
      }
    }
    return list;
  }

  /**
   * This method is static as it is called from the shutdown hook at the ExecDriver.
   */
  public static void cleanUp(String jobID, Configuration config) {
    StatsAggregator statsAggregator;
    String statsImplementationClass = HiveConf.getVar(config, HiveConf.ConfVars.HIVESTATSDBCLASS);
    StatsFactory.setImplementation(statsImplementationClass, config);
    statsAggregator = StatsFactory.getStatsAggregator();
    if (statsAggregator.connect(config)) {
      statsAggregator.cleanUp(jobID + Path.SEPARATOR); // Adding the path separator to avoid an Id being a prefix of another ID
    }
  }
}
