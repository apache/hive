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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
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
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * StatsTask implementation. StatsTask mainly deals with "collectable" stats. These are
 * stats that require data scanning and are collected during query execution (unless the user
 * explicitly requests data scanning just for the purpose of stats computation using the "ANALYZE"
 * command. All other stats are computed directly by the MetaStore. The rationale being that the
 * MetaStore layer covers all Thrift calls and provides better guarantees about the accuracy of
 * those stats.
 **/
public class StatsTask extends Task<StatsWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static transient final Log LOG = LogFactory.getLog(StatsTask.class);

  private Table table;
  private List<LinkedHashMap<String, String>> dpPartSpecs;

  public StatsTask() {
    super();
    dpPartSpecs = null;
  }

  /**
   *
   * Statistics for a Partition or Unpartitioned Table
   *
   */
  class Statistics {
    Map<String, LongWritable> stats;

    public Statistics() {
      stats = new HashMap<String, LongWritable>();
      for (String statType : StatsSetupConst.supportedStats) {
        stats.put(statType, new LongWritable(0L));
      }
    }

    public Statistics(Map<String, Long> st) {
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
      return org.apache.commons.lang.StringUtils.join(StatsSetupConst.supportedStats, ", ");
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

  private int aggregateStats() {

    StatsAggregator statsAggregator = null;
    int ret = 0;

    try {
      // Stats setup:
      Warehouse wh = new Warehouse(conf);

      if (!this.getWork().getNoStatsAggregator()) {
        String statsImplementationClass = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
        StatsFactory.setImplementation(statsImplementationClass, conf);
        if (work.isNoScanAnalyzeCommand()){
          // initialize stats publishing table for noscan which has only stats task
          // the rest of MR task following stats task initializes it in ExecDriver.java
          StatsPublisher statsPublisher = StatsFactory.getStatsPublisher();
          if (!statsPublisher.init(conf)) { // creating stats table if not exists
            if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
              throw
                new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
            }
          }
        }
        statsAggregator = StatsFactory.getStatsAggregator();
        // manufacture a StatsAggregator
        if (!statsAggregator.connect(conf)) {
          throw new HiveException("StatsAggregator connect failed " + statsImplementationClass);
        }
      }

      Statistics tblStats = new Statistics();

      org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
      Map<String, String> parameters = tTable.getParameters();

      boolean tableStatsExist = this.existStats(parameters);

      for (String statType : StatsSetupConst.supportedStats) {
        if (parameters.containsKey(statType)) {
          tblStats.setStat(statType, Long.parseLong(parameters.get(statType)));
        }
      }

      List<Partition> partitions = getPartitionsList();
      boolean atomic = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_ATOMIC);
      int maxPrefixLength = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.HIVE_STATS_KEY_PREFIX_MAX_LENGTH);

      if (partitions == null) {
        // non-partitioned tables:
        if (!tableStatsExist && atomic) {
          return 0;
        }
        // In case of a non-partitioned table, the key for stats temporary store is "rootDir"
        if (statsAggregator != null) {
          String aggKey = Utilities.getHashedStatsPrefix(work.getAggKey(), maxPrefixLength);
          updateStats(StatsSetupConst.statsRequireCompute, tblStats, statsAggregator, parameters,
              aggKey, atomic);
          statsAggregator.cleanUp(aggKey);
        }
        // The collectable stats for the aggregator needs to be cleared.
        // For eg. if a file is being loaded, the old number of rows are not valid
        else if (work.isClearAggregatorStats()) {
          for (String statType : StatsSetupConst.statsRequireCompute) {
            if (parameters.containsKey(statType)) {
              tblStats.setStat(statType, 0L);
            }
          }
        }

        // write table stats to metastore
        parameters = tTable.getParameters();
        for (String statType : StatsSetupConst.statsRequireCompute) {
          parameters.put(statType, Long.toString(tblStats.getStat(statType)));
        }
        parameters.put(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, StatsSetupConst.TRUE);
        tTable.setParameters(parameters);

        String tableFullName = table.getDbName() + "." + table.getTableName();

        db.alterTable(tableFullName, new Table(tTable));

        console.printInfo("Table " + tableFullName + " stats: [" + tblStats.toString() + ']');
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
          for (String statType : StatsSetupConst.supportedStats) {
            Long val = parameters.containsKey(statType) ? Long.parseLong(parameters.get(statType))
                : 0L;
            currentValues.put(statType, val);
          }

          //
          // get the new partition stats
          //
          Statistics newPartStats = new Statistics();

          // In that case of a partition, the key for stats temporary store is
          // "rootDir/[dynamic_partition_specs/]%"
          String partitionID = Utilities.getHashedStatsPrefix(
              work.getAggKey() + Warehouse.makePartPath(partn.getSpec()), maxPrefixLength);

          LOG.info("Stats aggregator : " + partitionID);

          if (statsAggregator != null) {
            updateStats(StatsSetupConst.statsRequireCompute, newPartStats, statsAggregator,
                parameters, partitionID, atomic);
            statsAggregator.cleanUp(partitionID);
          } else {
            for (String statType : StatsSetupConst.statsRequireCompute) {
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

          /**
           * calculate fast statistics
           */
          FileStatus[] partfileStatus = wh.getFileStatusesForPartition(tPart);
          newPartStats.setStat(StatsSetupConst.NUM_FILES, partfileStatus.length);
          long partSize = 0L;
          for (int i = 0; i < partfileStatus.length; i++) {
            partSize += partfileStatus[i].getLen();
          }
          newPartStats.setStat(StatsSetupConst.TOTAL_SIZE, partSize);

          //
          // update the metastore
          //
          for (String statType : StatsSetupConst.supportedStats) {
            long statValue = newPartStats.getStat(statType);
            if (statValue >= 0) {
              parameters.put(statType, Long.toString(newPartStats.getStat(statType)));
            }
          }

          parameters.put(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, StatsSetupConst.TRUE);
          tPart.setParameters(parameters);
          String tableFullName = table.getDbName() + "." + table.getTableName();
          db.alterPartition(tableFullName, new Partition(table, tPart));

          console.printInfo("Partition " + tableFullName + partn.getSpec() +
              " stats: [" + newPartStats.toString() + ']');
        }

      }

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

  private void updateStats(String[] statsList, Statistics stats,
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
