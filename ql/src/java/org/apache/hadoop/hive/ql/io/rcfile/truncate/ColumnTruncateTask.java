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

package org.apache.hadoop.hive.ql.io.rcfile.truncate;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper;
import org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHook;
import org.apache.hadoop.hive.ql.exec.mr.Throttle;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.MRJobConfig;

@SuppressWarnings( { "deprecation", "unchecked" })
public class ColumnTruncateTask extends Task<ColumnTruncateWork> implements Serializable,
    HadoopJobExecHook {

  private static final long serialVersionUID = 1L;

  protected transient JobConf job;
  protected HadoopJobExecHelper jobExecHelper;

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
    super.initialize(queryState, queryPlan, taskQueue, context);
    job = new JobConf(conf, ColumnTruncateTask.class);
    jobExecHelper = new HadoopJobExecHelper(job, this.console, this, this);
  }

  @Override
  public boolean requireLock() {
    return true;
  }

  boolean success = true;

  /**
   * start a new map-reduce job to do the truncation, almost the same as ExecDriver.
   */
  @Override
  public int execute() {
    HiveConf.setVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT,
        BucketizedHiveInputFormat.class.getName());
    success = true;
    HiveFileFormatUtils.prepareJobOutput(job);
    job.setOutputFormat(HiveOutputFormatImpl.class);
    job.setMapperClass(work.getMapperClass());

    Context ctx = context;
    boolean ctxCreated = false;
    if (ctx == null) {
      ctx = new Context(job);
      ctxCreated = true;
    }

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    if(work.getNumMapTasks() != null) {
      job.setNumMapTasks(work.getNumMapTasks());
    }

    // zero reducers
    job.setNumReduceTasks(0);
    // HIVE-23354 enforces that MR speculative execution is disabled
    job.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
    job.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);

    if (work.getMinSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZE, work
          .getMinSplitSize().longValue());
    }

    if (work.getInputformat() != null) {
      HiveConf.setVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT, work
          .getInputformat());
    }

    String inpFormat = HiveConf.getVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT);
    LOG.info("Using " + inpFormat);

    try {
      job.setInputFormat(JavaUtils.loadClass(inpFormat));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    Path outputPath = this.work.getOutputDir();
    Path tempOutPath = Utilities.toTempPath(outputPath);
    try {
      FileSystem fs = tempOutPath.getFileSystem(job);
      if (!fs.exists(tempOutPath)) {
        fs.mkdirs(tempOutPath);
      }
    } catch (IOException e) {
      setException(e);
      LOG.error("Can't make path " + outputPath, e);
      return 6;
    }

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    int returnVal = 0;
    RunningJob rj = null;

    boolean noName = StringUtils.isEmpty(job.get(MRJobConfig.JOB_NAME));

    String jobName = null;
    if (noName && this.getQueryPlan() != null) {
      int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
      jobName = Utilities.abbreviate(this.getQueryPlan().getQueryStr(),
          maxlen - 6);
    }

    if (noName) {
      // This is for a special case to ensure unit tests pass
      job.set(MRJobConfig.JOB_NAME, jobName != null ? jobName
          : "JOB" + ThreadLocalRandom.current().nextInt());
    }

    try {
      addInputPaths(job, work);

      MapredWork mrWork = new MapredWork();
      mrWork.setMapWork(work);
      Utilities.setMapRedWork(job, mrWork, ctx.getMRTmpPath());

      // remove the pwd from conf file so that job tracker doesn't show this
      // logs
      String pwd = HiveConf.getVar(job, HiveConf.ConfVars.METASTOREPWD);
      if (pwd != null) {
        HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, "HIVE");
      }
      JobClient jc = new JobClient(job);

      String addedJars = Utilities.getResourceFiles(job, SessionState.ResourceType.JAR);
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
      String mesg = rj != null ? ("Ended Job = " + rj.getJobID()) : "Job Submission failed";
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang3.StringUtils
      LOG.error(mesg, e);
      setException(e);

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
        ColumnTruncateMapper.jobClose(outputPath, success, job, console,
          work.getDynPartCtx(), null);
      } catch (Exception e) {
        LOG.warn("Failed while cleaning up ", e);
      } finally {
        HadoopJobExecHelper.runningJobs.remove(rj);
      }
    }

    return (returnVal);
  }

  private void addInputPaths(JobConf job, ColumnTruncateWork work) {
    FileInputFormat.addInputPath(job, work.getInputDir());
  }

  @Override
  public String getName() {
    return "RCFile ColumnTruncate";
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
