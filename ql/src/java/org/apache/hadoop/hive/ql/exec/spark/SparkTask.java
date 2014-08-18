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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

public class SparkTask extends Task<SparkWork> {
  private static final long serialVersionUID = 1L;
  private transient JobConf job;
  private transient ContentSummary inputSummary;

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
      configureNumberOfReducers();
      sparkSessionManager = SparkSessionManagerImpl.getInstance();
      sparkSession = SessionState.get().getSparkSession();
      sparkSession = sparkSessionManager.getSession(sparkSession, conf, true);
      SessionState.get().setSparkSession(sparkSession);

      rc = sparkSession.submit(driverContext, getWork());
    } catch (Exception e) {
      LOG.error("Failed to execute spark task.", e);
      return 1;
    }
    finally {
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

  /**
   * Set the number of reducers for the spark work.
   */
  private void configureNumberOfReducers() throws IOException {
    for (BaseWork baseWork : work.getAllWork()) {
      if (baseWork instanceof ReduceWork) {
        configureNumberOfReducers((ReduceWork) baseWork);
      }
    }

    console.printInfo("In order to change the average load for a reducer (in bytes):");
    console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname + "=<number>");
    console.printInfo("In order to limit the maximum number of reducers:");
    console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname + "=<number>");
    console.printInfo("In order to set a constant number of reducers:");
    console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS + "=<number>");
  }

  private void configureNumberOfReducers(ReduceWork rWork) throws IOException {
    // this is a temporary hack to fix things that are not fixed in the compiler
    Integer numReducersFromWork = rWork == null ? 0 : rWork.getNumReduceTasks();

    if (rWork == null) {
      console.printInfo("Number of reduce tasks is set to 0 since there's no reduce operator");
    } else {
      if (numReducersFromWork >= 0) {
        console.printInfo("Number of reduce tasks determined at compile time: "
          + rWork.getNumReduceTasks());
      } else if (job.getNumReduceTasks() > 0) {
        int reducers = job.getNumReduceTasks();
        rWork.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Defaulting to jobconf value of: "
            + reducers);
      } else {
        if (inputSummary == null) {
          inputSummary = Utilities.getInputSummary(driverContext.getCtx(), work.getMapWork(), null);
        }
        int reducers = Utilities.estimateNumberOfReducers(conf, inputSummary, work.getMapWork(),
          false);
        rWork.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Estimated from input data size: "
            + reducers);
      }
    }
  }
}
