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
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.counter.SparkCounters;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobMonitor;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

public class SparkTask extends Task<SparkWork> {
  private static final long serialVersionUID = 1L;
  private transient JobConf job;
  private transient ContentSummary inputSummary;
  private SparkCounters sparkCounters;

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);
    job = new JobConf(conf, SparkTask.class);
  }

  @Override
  public int execute(DriverContext driverContext) {

    int rc = 1;
    SparkSession sparkSession = null;
    SparkSessionManager sparkSessionManager = null;
    try {
      printConfigInfo();
      sparkSessionManager = SparkSessionManagerImpl.getInstance();
      sparkSession = SessionState.get().getSparkSession();

      // Spark configurations are updated close the existing session
      if(conf.getSparkConfigUpdated()){
        sparkSessionManager.closeSession(sparkSession);
        sparkSession =  null;
        conf.setSparkConfigUpdated(false);
      }
      sparkSession = sparkSessionManager.getSession(sparkSession, conf, true);
      SessionState.get().setSparkSession(sparkSession);
      SparkWork sparkWork = getWork();

      // We need to pre register spark counters for table statistic collection related query.
      String statsImpl = HiveConf.getVar(conf, HiveConf.ConfVars.HIVESTATSDBCLASS);
      StatsTask statsTask = getStatsTaskInChildTasks(this);
      if (statsImpl.equalsIgnoreCase("counter") && statsTask != null) {
        sparkWork.setRequiredCounterPrefix(getRequiredCounterPrefix(statsTask));
      }

      SparkJobRef jobRef = sparkSession.submit(driverContext, sparkWork);
      sparkCounters = jobRef.getSparkJobStatus().getCounter();
      SparkJobMonitor monitor = new SparkJobMonitor(jobRef.getSparkJobStatus());
      monitor.startMonitor();
      console.printInfo(sparkCounters.toString());
      rc = 0;
    } catch (Exception e) {
      LOG.error("Failed to execute spark task.", e);
      return 1;
    } finally {
      if (sparkSession != null && sparkSessionManager != null) {
        rc = close(rc);
        try {
          sparkSessionManager.returnSession(sparkSession);
        } catch(HiveException ex) {
          LOG.error("Failed to return the session to SessionManager", ex);
        }
      }
    }
    return rc;
  }

  /**
   * close will move the temp files into the right place for the fetch
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
    SparkWork work = getWork();

    // framework expects MapWork instances that have no physical parents (i.e.: union parent is
    // fine, broadcast parent isn't)
    for (BaseWork w: work.getAllWorkUnsorted()) {
      if (w instanceof MapWork) {
        List<BaseWork> parents = work.getParents(w);
        boolean candidate = true;
        for (BaseWork parent: parents) {
          if (!(parent instanceof UnionWork)) {
            candidate = false;
          }
        }
        if (candidate) {
          result.add((MapWork)w);
        }
      }
    }
    return result;
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

  private List<String> getRequiredCounterPrefix(StatsTask statsTask) throws HiveException, MetaException {
    List<String> prefixs = new LinkedList<String>();
    StatsWork statsWork = statsTask.getWork();
    String prefix = getPrefix(statsWork);
    List<Partition> partitions = getPartitionsList(statsWork);
    int maxPrefixLength = StatsFactory.getMaxPrefixLength(conf);

    if (partitions == null) {
      prefixs.add(Utilities.getHashedStatsPrefix(prefix, maxPrefixLength));
    } else {
      for (Partition partition : partitions) {
        String prefixWithPartition = Utilities.join(prefix, Warehouse.makePartPath(partition.getSpec()));
        prefixs.add(Utilities.getHashedStatsPrefix(prefixWithPartition, maxPrefixLength));
      }
    }

    return prefixs;
  }

  private String getPrefix(StatsWork work) throws HiveException {
      String tableName;
      if (work.getLoadTableDesc() != null) {
        tableName = work.getLoadTableDesc().getTable().getTableName();
      } else if (work.getTableSpecs() != null) {
        tableName = work.getTableSpecs().tableName;
      } else {
        tableName = work.getLoadFileDesc().getDestinationCreateTable();
      }
    Table table = null;
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
        return (StatsTask)task;
      } else {
        Task<? extends Serializable> childTask = getStatsTaskInChildTasks(task);
        if (childTask instanceof StatsTask) {
          return (StatsTask)childTask;
        } else {
          continue;
        }
      }
    }

    return null;
  }

  private List<Partition> getPartitionsList(StatsWork work) throws HiveException {
    if (work.getLoadFileDesc() != null) {
      return null; //we are in CTAS, so we know there are no partitions
    }
    Table table;
    List<Partition> list = new ArrayList<Partition>();

    if (work.getTableSpecs() != null) {

      // ANALYZE command
      BaseSemanticAnalyzer.tableSpec tblSpec = work.getTableSpecs();
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
        // we could not get dynamic partition information before SparkTask execution.
      } else { // static partition
        Partition partn = db.getPartition(table, tbd.getPartitionSpec(), false);
        list.add(partn);
      }
    }
    return list;
  }
}
