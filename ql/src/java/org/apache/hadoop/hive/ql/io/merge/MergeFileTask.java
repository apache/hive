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

package org.apache.hadoop.hive.ql.io.merge;

import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHook;
import org.apache.hadoop.hive.ql.exec.mr.Throttle;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.IOException;
import java.io.Serializable;

/**
 * Task for fast merging of ORC and RC files.
 */
public class MergeFileTask extends Task<MergeFileWork> implements Serializable,
    HadoopJobExecHook {

  private transient JobConf job;
  private HadoopJobExecHelper jobExecHelper;
  private boolean success = true;

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan,
      DriverContext driverContext, CompilationOpContext opContext) {
    super.initialize(queryState, queryPlan, driverContext, opContext);
    job = new JobConf(conf, MergeFileTask.class);
    jobExecHelper = new HadoopJobExecHelper(job, this.console, this, this);
  }

  @Override
  public boolean requireLock() {
    return true;
  }

  /**
   * start a new map-reduce job to do the merge, almost the same as ExecDriver.
   */
  @Override
  public int execute(DriverContext driverContext) {

    Context ctx = driverContext.getCtx();
    boolean ctxCreated = false;
    RunningJob rj = null;
    int returnVal = 0;

    try {
      if (ctx == null) {
        ctx = new Context(job);
        ctxCreated = true;
      }

      HiveFileFormatUtils.prepareJobOutput(job);
      job.setInputFormat(work.getInputformatClass());
      job.setOutputFormat(HiveOutputFormatImpl.class);
      job.setMapperClass(MergeFileMapper.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      job.setNumReduceTasks(0);

      // create the temp directories
      Path outputPath = work.getOutputDir();
      Path tempOutPath = Utilities.toTempPath(outputPath);
      FileSystem fs = tempOutPath.getFileSystem(job);
      if (!fs.exists(tempOutPath)) {
        fs.mkdirs(tempOutPath);
      }

      ExecDriver.propagateSplitSettings(job, work);

      // set job name
      boolean noName = StringUtils.isEmpty(job.get(MRJobConfig.JOB_NAME));

      String jobName = null;
      if (noName && this.getQueryPlan() != null) {
        int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
        jobName = Utilities.abbreviate(this.getQueryPlan().getQueryStr(),
            maxlen - 6);
      }

      if (noName) {
        // This is for a special case to ensure unit tests pass
        job.set(MRJobConfig.JOB_NAME,
            jobName != null ? jobName : "JOB" + Utilities.randGen.nextInt());
      }

      // add input path
      addInputPaths(job, work);

      // serialize work
      Utilities.setMapWork(job, work, ctx.getMRTmpPath(), true);

      // remove pwd from conf file so that job tracker doesn't show this logs
      String pwd = HiveConf.getVar(job, HiveConf.ConfVars.METASTOREPWD);
      if (pwd != null) {
        HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, "HIVE");
      }

      // submit the job
      JobClient jc = new JobClient(job);

      String addedJars = Utilities.getResourceFiles(job,
          SessionState.ResourceType.JAR);
      if (!addedJars.isEmpty()) {
        job.set("tmpjars", addedJars);
      }

      // make this client wait if job trcker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      // Finally SUBMIT the JOB!
      rj = jc.submitJob(job);
      this.jobID = rj.getJobID();
      returnVal = jobExecHelper.progress(rj, jc, ctx);
      success = (returnVal == 0);

    } catch (Exception e) {
      setException(e);
      String mesg = " with exception '" + Utilities.getNameMessage(e) + "'";
      if (rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }

      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      console.printError(mesg, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));

      success = false;
      returnVal = 1;
    } finally {
      try {
        if (ctxCreated) {
          ctx.clear();
        }
        if (rj != null) {
          if (returnVal != 0) {
            rj.killJob();
          }
        }
        // get the list of Dynamic partition paths
        if (rj != null) {
          if (work.getAliasToWork() != null) {
            for (Operator<? extends OperatorDesc> op : work.getAliasToWork()
                .values()) {
              op.jobClose(job, success);
            }
          }
        }
      } catch (Exception e) {
	// jobClose needs to execute successfully otherwise fail task
	LOG.warn("Job close failed ",e);
        if (success) {
          setException(e);
          success = false;
          returnVal = 3;
          String mesg = "Job Commit failed with exception '" +
              Utilities.getNameMessage(e) + "'";
          console.printError(mesg, "\n" +
              org.apache.hadoop.util.StringUtils.stringifyException(e));
        }
      } finally {
	HadoopJobExecHelper.runningJobs.remove(rj);
      }
    }

    return returnVal;
  }

  private void addInputPaths(JobConf job, MergeFileWork work) {
    for (Path path : work.getInputPaths()) {
      FileInputFormat.addInputPath(job, path);
    }
  }

  @Override
  public String getName() {
    return "MergeFileTask";
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public boolean checkFatalErrors(Counters ctrs, StringBuilder errMsg) {
    return false;
  }

  @Override
  public void logPlanProgress(SessionState ss) throws IOException {
    // no op
  }
}
