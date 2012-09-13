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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * StatsTask implementation.
 **/
public class StatsTask extends Task<StatsWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static transient final Log LOG = LogFactory.getLog(StatsTask.class);

  private Table table;
  private List<LinkedHashMap<String, String>> dpPartSpecs;

  private static final List<String> supportedStats = new ArrayList<String>();
  private static final List<String> collectableStats = new ArrayList<String>();
  private static final Map<String, String> nameMapping = new HashMap<String, String>();
  static {
    // supported statistics
    supportedStats.add(StatsSetupConst.NUM_FILES);
    supportedStats.add(StatsSetupConst.ROW_COUNT);
    supportedStats.add(StatsSetupConst.TOTAL_SIZE);
    supportedStats.add(StatsSetupConst.RAW_DATA_SIZE);

    // statistics that need to be collected throughout the execution
    collectableStats.add(StatsSetupConst.ROW_COUNT);
    collectableStats.add(StatsSetupConst.RAW_DATA_SIZE);

    nameMapping.put(StatsSetupConst.NUM_FILES, "num_files");
    nameMapping.put(StatsSetupConst.ROW_COUNT, "num_rows");
    nameMapping.put(StatsSetupConst.TOTAL_SIZE, "total_size");
    nameMapping.put(StatsSetupConst.RAW_DATA_SIZE, "raw_data_size");
  }

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
    Map<String, LongWritable> stats;

    public PartitionStatistics() {
      stats = new HashMap<String, LongWritable>();
      for (String statType : supportedStats) {
        stats.put(statType, new LongWritable(0L));
      }
    }

    public PartitionStatistics(Map<String, Long> st) {
      stats = new HashMap<String, LongWritable>();
      for (String statType : st.keySet()) {
        Long stValue = st.get(statType) == null ? 0L : st.get(statType);
        stats.put(statType, new LongWritable(stValue));
      }
    }

    public long getStat(String statType) {
      return stats.get(statType) == null ? 0L : stats.get(statType).get();
    }

    public void setStat(String statType, long value) {
      stats.put(statType, new LongWritable(value));
    }


    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (String statType : supportedStats) {
        sb.append(nameMapping.get(statType)).append(": ").append(stats.get(statType)).append(", ");
      }
      sb.delete(sb.length() - 2, sb.length());
      return sb.toString();
    }
  }

  /**
   * Table Level Statistics.
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
     *
     * @param oldStats
     *          The old statistics of a partition.
     * @param newStats
     *          The new statistics of a partition.
     */
    public void updateStats(PartitionStatistics oldStats, PartitionStatistics newStats) {
      deletePartitionStats(oldStats);
      addPartitionStats(newStats);
    }

    /**
     * Update the table level statistics when a new partition is added.
     *
     * @param newStats
     *          the new partition statistics.
     */
    public void addPartitionStats(PartitionStatistics newStats) {
      for (String statType : supportedStats) {
        LongWritable value = stats.get(statType);
        if (value == null) {
          stats.put(statType, new LongWritable(newStats.getStat(statType)));
        } else {
          value.set(value.get() + newStats.getStat(statType));
        }
      }
      this.numPartitions++;
    }

    /**
     * Update the table level statistics when an old partition is dropped.
     *
     * @param oldStats
     *          the old partition statistics.
     */
    public void deletePartitionStats(PartitionStatistics oldStats) {
      for (String statType : supportedStats) {
        LongWritable value = stats.get(statType);
        value.set(value.get() - oldStats.getStat(statType));
      }
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

    LOG.info("Executing stats task");
    // Make sure that it is either an ANALYZE, INSERT OVERWRITE or CTAS command
    short workComponentsPresent = 0;
    if (work.getLoadTableDesc() != null) {
      workComponentsPresent++;
    }
    if (work.getTableSpecs() != null) {
      workComponentsPresent++;
    }
    if (work.getLoadFileDesc() != null) {
      workComponentsPresent++;
    }

    assert (workComponentsPresent == 1);

    String tableName = "";
    try {
      if (work.getLoadTableDesc() != null) {
        tableName = work.getLoadTableDesc().getTable().getTableName();
      } else if (work.getTableSpecs() != null){
        tableName = work.getTableSpecs().tableName;
      } else {
        tableName = work.getLoadFileDesc().getDestinationCreateTable();
      }

      table = db.getTable(tableName);

    } catch (HiveException e) {
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

    StatsAggregator statsAggregator = null;
    int ret = 0;

    try {
      // Stats setup:
      Warehouse wh = new Warehouse(conf);
      FileSystem fileSys;
      FileStatus[] fileStatus;

      if (!this.getWork().getNoStatsAggregator()) {
        String statsImplementationClass = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
        StatsFactory.setImplementation(statsImplementationClass, conf);
        statsAggregator = StatsFactory.getStatsAggregator();
        // manufacture a StatsAggregator
        if (!statsAggregator.connect(conf)) {
          throw new HiveException("StatsAggregator connect failed " + statsImplementationClass);
        }
      }

      TableStatistics tblStats = new TableStatistics();

      org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
      Map<String, String> parameters = tTable.getParameters();

      boolean tableStatsExist = this.existStats(parameters);

      for (String statType : supportedStats) {
        if (parameters.containsKey(statType)) {
          tblStats.setStat(statType, Long.parseLong(parameters.get(statType)));
        }
      }

      if (parameters.containsKey(StatsSetupConst.NUM_PARTITIONS)) {
        tblStats.setNumPartitions(Integer.parseInt(parameters.get(StatsSetupConst.NUM_PARTITIONS)));
      }

      List<Partition> partitions = getPartitionsList();
      boolean atomic = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_ATOMIC);

      if (partitions == null) {
        // non-partitioned tables:
        if (!tableStatsExist && atomic) {
          return 0;
        }
        Path tablePath = wh.getTablePath(db.getDatabase(table.getDbName()), table.getTableName());
        fileSys = tablePath.getFileSystem(conf);
        fileStatus = Utilities.getFileStatusRecurse(tablePath, 1, fileSys);

        tblStats.setStat(StatsSetupConst.NUM_FILES, fileStatus.length);
        long tableSize = 0L;
        for (int i = 0; i < fileStatus.length; i++) {
          tableSize += fileStatus[i].getLen();
        }
        tblStats.setStat(StatsSetupConst.TOTAL_SIZE, tableSize);

        // In case of a non-partitioned table, the key for stats temporary store is "rootDir"
        if (statsAggregator != null) {
          updateStats(collectableStats, tblStats, statsAggregator, parameters,
              work.getAggKey(), atomic);
          statsAggregator.cleanUp(work.getAggKey());
        }
        // The collectable stats for the aggregator needs to be cleared.
        // For eg. if a file is being loaded, the old number of rows are not valid
        else if (work.isClearAggregatorStats()) {
          for (String statType : collectableStats) {
            if (parameters.containsKey(statType)) {
              tblStats.setStat(statType, 0L);
            }
          }
        }
      } else {
        // Partitioned table:
        // Need to get the old stats of the partition
        // and update the table stats based on the old and new stats.
        for (Partition partn : partitions) {
          //
          // get the old partition stats
          //
          org.apache.hadoop.hive.metastore.api.Partition tPart = partn.getTPartition();
          parameters = tPart.getParameters();

          boolean hasStats = this.existStats(parameters);
          if (!hasStats && atomic) {
            continue;
          }

          Map<String, Long> currentValues = new HashMap<String, Long>();
          for (String statType : supportedStats) {
            Long val = parameters.containsKey(statType) ? Long.parseLong(parameters.get(statType))
                : 0L;
            currentValues.put(statType, val);
          }

          //
          // get the new partition stats
          //
          PartitionStatistics newPartStats = new PartitionStatistics();

          // In that case of a partition, the key for stats temporary store is
          // "rootDir/[dynamic_partition_specs/]%"
          String partitionID = work.getAggKey() + Warehouse.makePartPath(partn.getSpec());

          LOG.info("Stats aggregator : " + partitionID);

          if (statsAggregator != null) {
            updateStats(collectableStats, newPartStats, statsAggregator,
                parameters, partitionID, atomic);
          } else {
            for (String statType : collectableStats) {
              // The collectable stats for the aggregator needs to be cleared.
              // For eg. if a file is being loaded, the old number of rows are not valid
              if (work.isClearAggregatorStats()) {
                if (parameters.containsKey(statType)) {
                  newPartStats.setStat(statType, 0L);
                }
              }
              else {
                newPartStats.setStat(statType, currentValues.get(statType));
              }
            }
          }

          fileSys = partn.getPartitionPath().getFileSystem(conf);
          fileStatus = Utilities.getFileStatusRecurse(partn.getPartitionPath(), 1, fileSys);
          newPartStats.setStat(StatsSetupConst.NUM_FILES, fileStatus.length);

          long partitionSize = 0L;
          for (int i = 0; i < fileStatus.length; i++) {
            partitionSize += fileStatus[i].getLen();
          }
          newPartStats.setStat(StatsSetupConst.TOTAL_SIZE, partitionSize);

          if (hasStats) {
            PartitionStatistics oldPartStats = new PartitionStatistics(currentValues);
            tblStats.updateStats(oldPartStats, newPartStats);
          } else {
            tblStats.addPartitionStats(newPartStats);
          }

          //
          // update the metastore
          //
          for (String statType : supportedStats) {
            long statValue = newPartStats.getStat(statType);
            if (statValue >= 0) {
              parameters.put(statType, Long.toString(newPartStats.getStat(statType)));
            }
          }

          tPart.setParameters(parameters);
          String tableFullName = table.getDbName() + "." + table.getTableName();
          db.alterPartition(tableFullName, new Partition(table, tPart));

          if (statsAggregator != null) {
            statsAggregator.cleanUp(partitionID);
          }

          console.printInfo("Partition " + tableFullName + partn.getSpec() +
              " stats: [" + newPartStats.toString() + ']');
        }

      }

      //
      // write table stats to metastore
      //
      parameters = tTable.getParameters();
      for (String statType : supportedStats) {
        parameters.put(statType, Long.toString(tblStats.getStat(statType)));
      }
      parameters.put(StatsSetupConst.NUM_PARTITIONS, Integer.toString(tblStats.getNumPartitions()));
      tTable.setParameters(parameters);

      String tableFullName = table.getDbName() + "." + table.getTableName();

      db.alterTable(tableFullName, new Table(tTable));

      console.printInfo("Table " + tableFullName + " stats: [" + tblStats.toString() + ']');

    } catch (Exception e) {
      console.printInfo("[Warning] could not update stats.",
          "Failed with exception " + e.getMessage() + "\n"
              + StringUtils.stringifyException(e));

      // Fail the query if the stats are supposed to be reliable
      if (work.isStatsReliable()) {
        ret = 1;
      }
    } finally {
      if (statsAggregator != null) {
        statsAggregator.closeConnection();
      }
    }
    // The return value of 0 indicates success,
    // anything else indicates failure
    return ret;
  }

  private boolean existStats(Map<String, String> parameters) {
    return parameters.containsKey(StatsSetupConst.ROW_COUNT)
        || parameters.containsKey(StatsSetupConst.NUM_FILES)
        || parameters.containsKey(StatsSetupConst.TOTAL_SIZE)
        || parameters.containsKey(StatsSetupConst.RAW_DATA_SIZE)
        || parameters.containsKey(StatsSetupConst.NUM_PARTITIONS);
  }

  private void updateStats(List<String> statsList, PartitionStatistics stats,
      StatsAggregator statsAggregator, Map<String, String> parameters,
      String aggKey, boolean atomic) throws HiveException {

    String value;
    Long longValue;
    for (String statType : statsList) {
      value = statsAggregator.aggregateStats(aggKey, statType);
      if (value != null) {
        longValue = Long.parseLong(value);

        if (work.getLoadTableDesc() != null &&
            !work.getLoadTableDesc().getReplace()) {
          String originalValue = parameters.get(statType);
          if (originalValue != null) {
            longValue += Long.parseLong(originalValue);
          }
        }
        stats.setStat(statType, longValue);
      } else {
        if (atomic) {
          throw new HiveException("StatsAggregator failed to get statistics.");
        }
      }
    }
  }

  /**
   * Get the list of partitions that need to update statistics.
   * TODO: we should reuse the Partitions generated at compile time
   * since getting the list of partitions is quite expensive.
   *
   * @return a list of partitions that need to update statistics.
   * @throws HiveException
   */
  private List<Partition> getPartitionsList() throws HiveException {
    if (work.getLoadFileDesc() != null) {
      return null; //we are in CTAS, so we know there are no partitions
    }

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
        // If no dynamic partitions are generated, dpPartSpecs may not be initialized
        if (dpPartSpecs != null) {
          // load the list of DP partitions and return the list of partition specs
          for (LinkedHashMap<String, String> partSpec : dpPartSpecs) {
            Partition partn = db.getPartition(table, partSpec, false);
            list.add(partn);
          }
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
      statsAggregator.cleanUp(jobID + Path.SEPARATOR); // Adding the path separator to avoid an Id
                                                       // being a prefix of another ID
      statsAggregator.closeConnection();
    }
  }
}
