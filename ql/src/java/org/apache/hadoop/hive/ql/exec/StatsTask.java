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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
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
import org.apache.hadoop.hive.ql.stats.StatsCollectionTaskIndependent;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
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
      if (!getWork().getNoStatsAggregator() && !getWork().isNoScanAnalyzeCommand()) {
        try {
          statsAggregator = createStatsAggregator(conf);
        } catch (HiveException e) {
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
            throw e;
          }
          console.printError(ErrorMsg.STATS_SKIPPING_BY_ERROR.getErrorCodedMsg(e.toString()));
        }
      }

      List<Partition> partitions = getPartitionsList();
      boolean atomic = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_ATOMIC);

      String tableFullName = table.getDbName() + "." + table.getTableName();

      int maxPrefixLength = StatsFactory.getMaxPrefixLength(conf);

      // "counter" or "fs" type does not need to collect stats per task
      boolean taskIndependent = statsAggregator instanceof StatsCollectionTaskIndependent;
      if (partitions == null) {
        org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
        Map<String, String> parameters = tTable.getParameters();
        // non-partitioned tables:
        if (!existStats(parameters) && atomic) {
          return 0;
        }

        // The collectable stats for the aggregator needs to be cleared.
        // For eg. if a file is being loaded, the old number of rows are not valid
        if (work.isClearAggregatorStats()) {
          clearStats(parameters);
        }

        if (statsAggregator != null) {
          String prefix = getAggregationPrefix(taskIndependent, table, null);
          updateStats(statsAggregator, parameters, prefix, maxPrefixLength, atomic);
        }

        updateQuickStats(wh, parameters, tTable.getSd());

        // write table stats to metastore
        parameters.put(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, StatsSetupConst.TRUE);

        db.alterTable(tableFullName, new Table(tTable));

        console.printInfo("Table " + tableFullName + " stats: [" + toString(parameters) + ']');
      } else {
        // Partitioned table:
        // Need to get the old stats of the partition
        // and update the table stats based on the old and new stats.
        List<Partition> updates = new ArrayList<Partition>();
        for (Partition partn : partitions) {
          //
          // get the old partition stats
          //
          org.apache.hadoop.hive.metastore.api.Partition tPart = partn.getTPartition();
          Map<String, String> parameters = tPart.getParameters();
          if (!existStats(parameters) && atomic) {
            continue;
          }

          // The collectable stats for the aggregator needs to be cleared.
          // For eg. if a file is being loaded, the old number of rows are not valid
          if (work.isClearAggregatorStats()) {
            clearStats(parameters);
          }

          if (statsAggregator != null) {
            String prefix = getAggregationPrefix(taskIndependent, table, partn);
            updateStats(statsAggregator, parameters, prefix, maxPrefixLength, atomic);
          }

          updateQuickStats(wh, parameters, tPart.getSd());

          parameters.put(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, StatsSetupConst.TRUE);
          updates.add(new Partition(table, tPart));

          console.printInfo("Partition " + tableFullName + partn.getSpec() +
              " stats: [" + toString(parameters) + ']');
        }
        if (!updates.isEmpty()) {
          db.alterPartitions(tableFullName, updates);
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

  private String getAggregationPrefix(boolean counter, Table table, Partition partition)
      throws MetaException {
    if (!counter && partition == null) {
      return work.getAggKey();
    }
    StringBuilder prefix = new StringBuilder();
    if (counter) {
      // prefix is of the form dbName.tblName
      prefix.append(table.getDbName()).append('.').append(table.getTableName());
    } else {
      // In case of a non-partitioned table, the key for stats temporary store is "rootDir"
      prefix.append(work.getAggKey());
    }
    if (partition != null) {
      return Utilities.join(prefix.toString(), Warehouse.makePartPath(partition.getSpec()));
    }
    return prefix.toString();
  }

  private StatsAggregator createStatsAggregator(HiveConf conf) throws HiveException {
    String statsImpl = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
    StatsFactory factory = StatsFactory.newFactory(statsImpl, conf);
    if (factory == null) {
      throw new HiveException(ErrorMsg.STATSPUBLISHER_NOT_OBTAINED.getErrorCodedMsg());
    }
    // initialize stats publishing table for noscan which has only stats task
    // the rest of MR task following stats task initializes it in ExecDriver.java
    StatsPublisher statsPublisher = factory.getStatsPublisher();
    if (!statsPublisher.init(conf)) { // creating stats table if not exists
      throw new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
    }
    Task sourceTask = getWork().getSourceTask();
    if (sourceTask == null) {
      throw new HiveException(ErrorMsg.STATSAGGREGATOR_SOURCETASK_NULL.getErrorCodedMsg());
    }
    // manufacture a StatsAggregator
    StatsAggregator statsAggregator = factory.getStatsAggregator();
    if (!statsAggregator.connect(conf, sourceTask)) {
      throw new HiveException(ErrorMsg.STATSAGGREGATOR_CONNECTION_ERROR.getErrorCodedMsg(statsImpl));
    }
    return statsAggregator;
  }

  private boolean existStats(Map<String, String> parameters) {
    return parameters.containsKey(StatsSetupConst.ROW_COUNT)
        || parameters.containsKey(StatsSetupConst.NUM_FILES)
        || parameters.containsKey(StatsSetupConst.TOTAL_SIZE)
        || parameters.containsKey(StatsSetupConst.RAW_DATA_SIZE)
        || parameters.containsKey(StatsSetupConst.NUM_PARTITIONS);
  }

  private void updateStats(StatsAggregator statsAggregator,
      Map<String, String> parameters, String prefix, int maxPrefixLength, boolean atomic)
      throws HiveException {

    String aggKey = Utilities.getHashedStatsPrefix(prefix, maxPrefixLength);

    for (String statType : StatsSetupConst.statsRequireCompute) {
      String value = statsAggregator.aggregateStats(aggKey, statType);
      if (value != null && !value.isEmpty()) {
        long longValue = Long.parseLong(value);

        if (work.getLoadTableDesc() != null &&
            !work.getLoadTableDesc().getReplace()) {
          String originalValue = parameters.get(statType);
          if (originalValue != null && !originalValue.equals("-1")) {
            longValue += Long.parseLong(originalValue); // todo: invalid + valid = invalid
          }
        }
        parameters.put(statType, String.valueOf(longValue));
      } else {
        if (atomic) {
          throw new HiveException(ErrorMsg.STATSAGGREGATOR_MISSED_SOMESTATS, statType);
        }
      }
    }
    statsAggregator.cleanUp(aggKey);
  }

  private void updateQuickStats(Warehouse wh, Map<String, String> parameters,
      StorageDescriptor desc) throws MetaException {
    /**
     * calculate fast statistics
     */
    FileStatus[] partfileStatus = wh.getFileStatusesForSD(desc);
    parameters.put(StatsSetupConst.NUM_FILES, String.valueOf(partfileStatus.length));
    long partSize = 0L;
    for (int i = 0; i < partfileStatus.length; i++) {
      partSize += partfileStatus[i].getLen();
    }
    parameters.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(partSize));
  }

  private void clearStats(Map<String, String> parameters) {
    for (String statType : StatsSetupConst.supportedStats) {
      if (parameters.containsKey(statType)) {
        parameters.put(statType, "0");
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
}
