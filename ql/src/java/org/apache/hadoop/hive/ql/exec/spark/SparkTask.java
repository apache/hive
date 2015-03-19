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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistic;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticGroup;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistics;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.spark.counter.SparkCounters;

import com.google.common.collect.Lists;

public class SparkTask extends Task<SparkWork> {
  private static final String CLASS_NAME = SparkTask.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);
  private static final LogHelper console = new LogHelper(LOG);
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private static final long serialVersionUID = 1L;
  private SparkCounters sparkCounters;

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);
  }

  @Override
  public int execute(DriverContext driverContext) {

    int rc = 0;
    SparkSession sparkSession = null;
    SparkSessionManager sparkSessionManager = null;
    try {
      printConfigInfo();
      sparkSessionManager = SparkSessionManagerImpl.getInstance();
      sparkSession = SparkUtilities.getSparkSession(conf, sparkSessionManager);

      SparkWork sparkWork = getWork();
      sparkWork.setRequiredCounterPrefix(getCounterPrefixes());

      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_SUBMIT_JOB);
      SparkJobRef jobRef = sparkSession.submit(driverContext, sparkWork);
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_SUBMIT_JOB);

      addToHistory(jobRef);
      rc = jobRef.monitorJob();
      SparkJobStatus sparkJobStatus = jobRef.getSparkJobStatus();
      if (rc == 0) {
        sparkCounters = sparkJobStatus.getCounter();
        // for RSC, we should get the counters after job has finished
        SparkStatistics sparkStatistics = sparkJobStatus.getSparkStatistics();
        if (LOG.isInfoEnabled() && sparkStatistics != null) {
          LOG.info(String.format("=====Spark Job[%s] statistics=====", jobRef.getJobId()));
          logSparkStatistic(sparkStatistics);
        }
      } else if (rc == 2) { // Cancel job if the monitor found job submission timeout.
        jobRef.cancelJob();
      }
      sparkJobStatus.cleanup();
    } catch (Exception e) {
      String msg = "Failed to execute spark task, with exception '" + Utilities.getNameMessage(e) + "'";

      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      console.printError(msg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      LOG.error(msg, e);
      rc = 1;
    } finally {
      Utilities.clearWork(conf);
      if (sparkSession != null && sparkSessionManager != null) {
        rc = close(rc);
        try {
          sparkSessionManager.returnSession(sparkSession);
        } catch (HiveException ex) {
          LOG.error("Failed to return the session to SessionManager", ex);
        }
      }
    }
    return rc;
  }

  private void addToHistory(SparkJobRef jobRef) {
    console.printInfo("Starting Spark Job = " + jobRef.getJobId());
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory()
	  .setQueryProperty(SessionState.get().getQueryId(), Keys.SPARK_JOB_ID, jobRef.getJobId());
    }
  }

  private void logSparkStatistic(SparkStatistics sparkStatistic) {
    Iterator<SparkStatisticGroup> groupIterator = sparkStatistic.getStatisticGroups();
    while (groupIterator.hasNext()) {
      SparkStatisticGroup group = groupIterator.next();
      LOG.info(group.getGroupName());
      Iterator<SparkStatistic> statisticIterator = group.getStatistics();
      while (statisticIterator.hasNext()) {
        SparkStatistic statistic = statisticIterator.next();
        LOG.info("\t" + statistic.getName() + ": " + statistic.getValue());
      }
    }
  }

  /**
   * Close will move the temp files into the right place for the fetch
   * task. If the job has failed it will clean up the files.
   */
  private int close(int rc) {
    try {
      List<BaseWork> ws = work.getAllWork();
      for (BaseWork w: ws) {
        for (Operator<?> op: w.getAllOperators()) {
          op.jobClose(conf, rc == 0);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if (rc == 0) {
        rc = 3;
        String mesg = "Job Commit failed with exception '"
            + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + StringUtils.stringifyException(e));
      }
    }
    return rc;
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "SPARK";
  }

  @Override
  public Collection<MapWork> getMapWork() {
    List<MapWork> result = Lists.newArrayList();
    for (BaseWork w : getWork().getRoots()) {
        result.add((MapWork) w);
    }
    return result;
  }

  @Override
  public Operator<? extends OperatorDesc> getReducer(MapWork mapWork) {
    List<BaseWork> children = getWork().getChildren(mapWork);
    if (children.size() != 1) {
      return null;
    }

    if (!(children.get(0) instanceof ReduceWork)) {
      return null;
    }

    return ((ReduceWork) children.get(0)).getReducer();
  }

  public SparkCounters getSparkCounters() {
    return sparkCounters;
  }

  /**
   * Set the number of reducers for the spark work.
   */
  private void printConfigInfo() throws IOException {

    console.printInfo("In order to change the average load for a reducer (in bytes):");
    console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname + "=<number>");
    console.printInfo("In order to limit the maximum number of reducers:");
    console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname + "=<number>");
    console.printInfo("In order to set a constant number of reducers:");
    console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS + "=<number>");
  }

  private Map<String, List<String>> getCounterPrefixes() throws HiveException, MetaException {
    Map<String, List<String>> counters = getOperatorCounters();
    StatsTask statsTask = getStatsTaskInChildTasks(this);
    String statsImpl = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
    // fetch table prefix if SparkTask try to gather table statistics based on counter.
    if (statsImpl.equalsIgnoreCase("counter") && statsTask != null) {
      List<String> prefixes = getRequiredCounterPrefix(statsTask);
      for (String prefix : prefixes) {
        List<String> counterGroup = counters.get(prefix);
        if (counterGroup == null) {
          counterGroup = new LinkedList<String>();
          counters.put(prefix, counterGroup);
        }
        counterGroup.add(StatsSetupConst.ROW_COUNT);
        counterGroup.add(StatsSetupConst.RAW_DATA_SIZE);
      }
    }
    return counters;
  }

  private List<String> getRequiredCounterPrefix(StatsTask statsTask) throws HiveException, MetaException {
    List<String> prefixs = new LinkedList<String>();
    StatsWork statsWork = statsTask.getWork();
    String tablePrefix = getTablePrefix(statsWork);
    List<Map<String, String>> partitionSpecs = getPartitionSpecs(statsWork);
    int maxPrefixLength = StatsFactory.getMaxPrefixLength(conf);

    if (partitionSpecs == null) {
      prefixs.add(Utilities.getHashedStatsPrefix(tablePrefix, maxPrefixLength));
    } else {
      for (Map<String, String> partitionSpec : partitionSpecs) {
        String prefixWithPartition = Utilities.join(tablePrefix, Warehouse.makePartPath(partitionSpec));
        prefixs.add(Utilities.getHashedStatsPrefix(prefixWithPartition, maxPrefixLength));
      }
    }

    return prefixs;
  }

  private String getTablePrefix(StatsWork work) throws HiveException {
      String tableName;
      if (work.getLoadTableDesc() != null) {
        tableName = work.getLoadTableDesc().getTable().getTableName();
      } else if (work.getTableSpecs() != null) {
        tableName = work.getTableSpecs().tableName;
      } else {
        tableName = work.getLoadFileDesc().getDestinationCreateTable();
      }
    Table table;
    try {
      table = db.getTable(tableName);
    } catch (HiveException e) {
      LOG.warn("Failed to get table:" + tableName);
      // For CTAS query, table does not exist in this period, just use table name as prefix.
      return tableName.toLowerCase();
    }
    return table.getDbName() + "." + table.getTableName();
  }

  private static StatsTask getStatsTaskInChildTasks(Task<? extends Serializable> rootTask) {

    List<Task<? extends Serializable>> childTasks = rootTask.getChildTasks();
    if (childTasks == null) {
      return null;
    }
    for (Task<? extends Serializable> task : childTasks) {
      if (task instanceof StatsTask) {
        return (StatsTask) task;
      } else {
        Task<? extends Serializable> childTask = getStatsTaskInChildTasks(task);
        if (childTask instanceof StatsTask) {
          return (StatsTask) childTask;
        } else {
          continue;
        }
      }
    }

    return null;
  }

  private List<Map<String, String>> getPartitionSpecs(StatsWork work) throws HiveException {
    if (work.getLoadFileDesc() != null) {
      return null; //we are in CTAS, so we know there are no partitions
    }
    Table table;
    List<Map<String, String>> partitionSpecs = new ArrayList<Map<String, String>>();

    if (work.getTableSpecs() != null) {

      // ANALYZE command
      TableSpec tblSpec = work.getTableSpecs();
      table = tblSpec.tableHandle;
      if (!table.isPartitioned()) {
        return null;
      }
      // get all partitions that matches with the partition spec
      List<Partition> partitions = tblSpec.partitions;
      if (partitions != null) {
        for (Partition partition : partitions) {
          partitionSpecs.add(partition.getSpec());
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
        // we could not get dynamic partition information before SparkTask execution.
      } else { // static partition
        partitionSpecs.add(tbd.getPartitionSpec());
      }
    }
    return partitionSpecs;
  }

  private Map<String, List<String>> getOperatorCounters() {
    String groupName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVECOUNTERGROUP);
    Map<String, List<String>> counters = new HashMap<String, List<String>>();
    List<String> hiveCounters = new LinkedList<String>();
    counters.put(groupName, hiveCounters);
    hiveCounters.add(Operator.HIVECOUNTERCREATEDFILES);
    // MapOperator is out of SparkWork, SparkMapRecordHandler use it to bridge
    // Spark transformation and Hive operators in SparkWork.
    for (MapOperator.Counter counter : MapOperator.Counter.values()) {
      hiveCounters.add(counter.toString());
    }
    SparkWork sparkWork = this.getWork();
    for (BaseWork work : sparkWork.getAllWork()) {
      for (Operator<? extends OperatorDesc> operator : work.getAllOperators()) {
        if (operator instanceof FileSinkOperator) {
          for (FileSinkOperator.Counter counter : FileSinkOperator.Counter.values()) {
            hiveCounters.add(counter.toString());
          }
        } else if (operator instanceof ReduceSinkOperator) {
          for (ReduceSinkOperator.Counter counter : ReduceSinkOperator.Counter.values()) {
            hiveCounters.add(counter.toString());
          }
        } else if (operator instanceof ScriptOperator) {
          for (ScriptOperator.Counter counter : ScriptOperator.Counter.values()) {
            hiveCounters.add(counter.toString());
          }
        } else if (operator instanceof JoinOperator) {
          for (JoinOperator.SkewkeyTableCounter counter : JoinOperator.SkewkeyTableCounter.values()) {
            hiveCounters.add(counter.toString());
          }
        }
      }
    }

    return counters;
  }
}
